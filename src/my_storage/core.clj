(ns my-storage.core
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io]
            [my-storage.encoding :as enc]
            [my-storage.wal :as wal]
            [my-storage.manifest :as manifest]
            [my-storage.sstable :as sstable]))

(defprotocol IKVStore
  (-put [this k v])
  (-get [this k])
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
    (println (format "[flush] %s (%d entries) / wal rotated"
                     (.getName file) (count imm)))
    file))

(defn- lookup
  [store k]
  (let [{:keys [memtable immutables sstables]} @(:state store)
        stats (:stats store)]
    (if-let [e (or (find memtable k)
                   (some #(find % k) immutables))]
      (val e)
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
      (flush-immutable! store (peek (:immutables new))))))

;; (defn stats [store] @(:stats store))

(defrecord LSMStore [state opts dir stats]
  IKVStore
  (-put [this k v]
    (wal/append! (:wal @state) (enc/record-bytes k v))
    (swap! state update :memtable assoc k v)
    (maybe-flush! this)
    this)
  (-get [this k]
    (lookup this k))
  (-delete [this k]
    (wal/append! (:wal @state) (enc/record-bytes k enc/tombstone))
    (swap! state update :memtable dissoc k)
    this)
  (-close [this]
    (wal/close! (:wal @state))
    (doseq [r (:sstables @state)]
      (sstable/close-reader! r))
    nil))

(defn put [store k v] (-put store k v))
(defn get [store k] (-get store k))
(defn delete [store k] (-delete store k))
(defn close [store] (-close store))

(defn open
  ([dir] (open dir {}))
  ([dir opts]
   (let [opts (merge {:flush-threshold 1000} opts)
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
