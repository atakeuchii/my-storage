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
        (swap! (:state store) update :immutables pop)
        (println (format "[sstable] wrote %s (%d entries)"
                         (.getName file)
                         (count imm)))
        file))))

(defn- mem-find
  "memtable → immutables(新しい順)の順に k を探す。見つかれば MapEntry(truthy)、無ければ nil"
  [snapshot k]
  (let [{:keys [memtable immutables]} snapshot]
    (or (find memtable k)
        (some #(find % k) immutables))))

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

(defrecord LSMStore [state wal opts dir]
  IKVStore
  (-put [this k v]
    (wal/append! wal (enc/record-bytes k v))
    (swap! state update :memtable assoc k v)
    (maybe-flush! this)
    this)
  (-get [this k]
    (when-let [e (mem-find @state k)]
      (val e)))
  (-delete [this k]
    (wal/append! wal (enc/record-bytes k enc/tombstone))
    (swap! state update :memtable dissoc k)
    this)
  (-close [this]
    (wal/close! wal)
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
     (let [wal-file (io/file d "wal.log")
           [mt good] (wal/replay wal-file)]
       (when (< good (.length wal-file))
         (wal/truncate! wal-file good))
       (->LSMStore (atom {:memtable mt :immutables []})
                   (wal/open wal-file)
                   opts
                   d)))))
