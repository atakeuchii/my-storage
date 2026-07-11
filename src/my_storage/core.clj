(ns my-storage.core
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io]
            [my-storage.compaction :as compaction]
            [my-storage.encoding :as enc]
            [my-storage.wal :as wal]
            [my-storage.manifest :as manifest]
            [my-storage.merge :as merge]
            [my-storage.sstable :as sstable])
  (:import [java.io File]))

(defprotocol IKVStore
  (-put [this k v])
  (-get [this k])
  (-scan [this start end])
  (-delete [this k])
  (-close [this]))

(defn- flush-immutable!
  [store imm]
  (let [dir (:dir store)
        file (sstable/next-sstable-file dir)]
    (sstable/write-sstable! file imm)
    (manifest/add-sstable! dir (.getName file))
    (let [reader (sstable/open-reader file)
          new-wal (wal/rotate! (:wal @(:state store)))]
      (swap! (:state store)
             (fn [s]
               (-> s
                   (update :immutables pop)
                   (update :sstables #(into [reader] %))
                   (assoc :wal new-wal)))))
    (when (:verbose (:opts store))
      (println (format "[flush] %s (%d entries) / wal rotated"
                       (.getName file) (count imm))))
    file))

(defn- lookup
  [store k]
  (let [{:keys [memtable immutables sstables]} @(:state store)
        stats (:stats store)]
    (if-let [e (or (find memtable k)
                   (some #(find % k) immutables))]
      (let [v (val e)]
        (if (= v enc/tombstone) nil v))
      (loop [ss (seq sstables)]
        (when ss
          (let [reader (first ss)]
            (if (sstable/might-contain? reader k)
              (do (swap! stats update :reads inc)
                  (let [v (sstable/sstable-get reader k)]
                    (cond
                      (= v enc/not-found) (recur (next ss))
                      (= v enc/tombstone) nil
                      :else v)))
              (do (swap! stats update :skips inc)
                  (recur (next ss))))))))))

(defn- compact-group!
  [store i j]
  (let [dir (:dir store)
        sstables (:sstables @(:state store))
        n (count sstables)
        group (subvec sstables i (inc j))
        drop-tomb? (= j (dec n))
        new-file (compaction/compact! dir group drop-tomb?)
        new-reader (sstable/open-reader new-file)
        new-sstables (vec (concat (subvec sstables 0 i)
                                  [new-reader]
                                  (subvec sstables (inc j))))]
    (manifest/save-manifest! dir
                             {:sstables (mapv #(.getName ^File (:file %))
                                              (reverse new-sstables))})
    (swap! (:state store) assoc :sstables new-sstables)
    (doseq [r group]
      (sstable/close-reader! r)
      (.delete ^File (:file r)))
    (when (:verbose (:opts store))
      (println (format "[compact] [%d..%d] %d files (drop-tomb=%s) -> %s"
                       i j (count group) drop-tomb? (.getName new-file))))
    new-reader))

(defn- maybe-compact!
  [store]
  (when-let [threshold (:compaction-threshold (:opts store))]
    (let [ratio (:compaction-size-ratio (:opts store))]
      (loop []
        (let [sstables (:sstables @(:state store))
              sizes (mapv (fn [r] [(.getName ^File (:file r))
                                   (.length ^File (:file r))])
                          sstables)]
          (when-let [[i j] (compaction/pick-compaction sizes threshold ratio)]
            (compact-group! store i j)
            (recur)))))))

(defn- maybe-flush!
  "memtable の件数が閾値以上なら、空 memtable にアトミックに切り替え、古い memtable を immutables の先頭(最新)へ退避する。"
  [store]
  (let [threshold (:flush-threshold (:opts store))
        [old new] (swap-vals! (:state store)
                              (fn [s]
                                (if (>= (count (:memtable s)) threshold)
                                  (-> s
                                      (update :immutables #(into [(:memtable s)] %))
                                      (assoc :memtable (sorted-map)))
                                  s)))]
    (when (> (count (:immutables new)) (count (:immutables old)))
      (flush-immutable! store (peek (:immutables new)))
      (maybe-compact! store))))

(defn- mem-range
  [sm start end]
  (->> (cond
         (and start end) (subseq sm >= start < end)
         start (subseq sm >= start)
         end (subseq sm < end)
         :else (seq sm))
       (map (fn [e] [(key e) (val e)]))))

(defn- scan* [store start end]
  (let [{:keys [memtable immutables sstables]} @(:state store)
        sources (concat [(mem-range memtable start end)]
                        (map #(mem-range % start end) immutables)
                        (map #(sstable/sstable-scan % start end) sstables))]
    (->> (merge/merge-sorted sources)
         (remove (fn [[_ v]] (= v enc/tombstone))))))


(defrecord LSMStore [state opts dir stats]
  IKVStore
  (-put [this k v]
    (wal/append! (:wal @state) (enc/record-bytes k v))
    (swap! state update :memtable assoc k v)
    (maybe-flush! this)
    this)
  (-get [this k]
    (lookup this k))
  (-scan [this start end]
    (scan* this start end))
  (-delete [this k]
    (wal/append! (:wal @state) (enc/record-bytes k enc/tombstone))
    (swap! state update :memtable assoc k enc/tombstone)
    (maybe-flush! this)
    this)
  (-close [this]
    (wal/close! (:wal @state))
    (doseq [r (:sstables @state)]
      (sstable/close-reader! r))
    nil))

(defn put [store k v] (-put store k v))
(defn get [store k] (-get store k))
(defn scan [store start end] (-scan store start end))
(defn delete [store k] (-delete store k))
(defn close [store] (-close store))
(defn compact! [store]
  (let [n (count (:sstables @(:state store)))]
    (when (pos? n)
      (compact-group! store 0 (dec n)))))

(defn open
  ([dir] (open dir {}))
  ([dir opts]
   (let [opts (merge {:flush-threshold 1000
                      :compaction-size-ratio 1.5}
                     opts)
         d (io/file dir)]
     (.mkdirs d)
     (let [m (manifest/load-manifest d)
           sstables (mapv #(sstable/open-reader (io/file d %))
                          (reverse (:sstables m)))
           wal-file (io/file d "wal.log")
           [mt good] (wal/replay wal-file)]
       (when (< good (.length wal-file))
         (wal/truncate! wal-file good))
       (->LSMStore (atom {:memtable mt :immutables [] :sstables sstables :wal (wal/open wal-file)})
                   opts
                   d
                   (atom {:reads 0 :skips 0}))))))
