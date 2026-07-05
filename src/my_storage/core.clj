(ns my-storage.core
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io]
            [my-storage.encoding :as enc]
            [my-storage.wal :as wal]
            [my-storage.sstable :as sstable]))

(defprotocol IKVStore
  (-put [this k v])
  (-get [this k])
  (-delete [this k])
  (-close [this]))

(defn- flush-oldest-immutable!
  "immutables の先頭(最古)を sstable として書き出し、immutables から削除する。"
  [store]
  (let [{:keys [immutables]} @(:state store)]
    (when-let [imm (peek immutables)]
      (let [file (sstable/next-sstable-file (:dir store))]
        (sstable/write-sstable! file imm)
        (let [reader (sstable/open-reader file)]
          (swap! (:state store)
                 (fn [s]
                   (-> s
                       (update :immutables pop)
                       (update :sstables #(into [reader] %))))))
        (println (format "[sstable] wrote %s (%d entries)"
                         (.getName file)
                         (count imm)))
        file))))

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
      (flush-oldest-immutable! store))))

;; (defn stats [store] @(:stats store))

(defrecord LSMStore [state wal opts dir stats]
  IKVStore
  (-put [this k v]
    (wal/append! wal (enc/record-bytes k v))
    (swap! state update :memtable assoc k v)
    (maybe-flush! this)
    this)
  (-get [this k]
    (lookup this k))
  (-delete [this k]
    (wal/append! wal (enc/record-bytes k enc/tombstone))
    (swap! state update :memtable dissoc k)
    this)
  (-close [this]
    (wal/close! wal)
    (doseq [r (:sstables @state)]
      (sstable/close-reader! r))
    nil))

(defn put [store k v] (-put store k v))
(defn get [store k] (-get store k))
(defn delete [store k] (-delete store k))
(defn close [store] (-close store))

(defn- load-sstables
  [dir]
  (->> (sstable/list-sstable-files dir)
       reverse
       (mapv sstable/open-reader)))

(defn open
  ([dir] (open dir {}))
  ([dir opts]
   (let [opts (merge {:flush-threshold 1000} opts)
         d (io/file dir)]
     (.mkdirs d)
     (let [wal-file (io/file d "wal.log")
           [mt good] (wal/replay wal-file)
           sstables (load-sstables d)]
       (when (< good (.length wal-file))
         (wal/truncate! wal-file good))
       (->LSMStore (atom {:memtable mt :immutables [] :sstables sstables})
                   (wal/open wal-file)
                   opts
                   d
                   (atom {:reads 0 :skips 0}))))))
