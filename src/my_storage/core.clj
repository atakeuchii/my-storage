(ns my-storage.core
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io])
  (:import [java.io FileOutputStream ByteArrayOutputStream DataOutputStream File RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.file Files]
           [java.util.zip CRC32]))

(defprotocol IKVStore
  (-put [this k v])
  (-get [this k])
  (-delete [this k])
  (-close [this]))

(defn- record-bytes
  "1レコードを [keyLen][key][valLen][value][crc] のバイト列にして返す。
   v が ::tombstone のときは valLen = -1 で value なし。"
  ^bytes [^String k v]
  (let [kb (.getBytes k "UTF-8")
        tomb? (= v :tombstone)
        ^bytes vb (when-not tomb? (.getBytes ^String v "UTF-8"))
        baos (ByteArrayOutputStream.)
        dos (DataOutputStream. baos)]
    (.writeInt dos (alength kb))
    (.write dos kb)
    (if tomb?
      (.writeInt dos (int -1))
      (do (.writeInt dos (alength vb))
          (.write dos vb)))
    (.flush dos)
    (let [payload (.toByteArray baos)
          crc (doto (CRC32.) (.update payload))]
      (.writeInt dos (unchecked-int (.getValue crc)))
      (.flush dos)
      (.toByteArray baos))))

(defn- try-read-record
  "buf の現在位置から1レコードを読む"
  [^ByteBuffer buf]
  (let [start (.position buf)]
    (try
      (let [key-len (.getInt buf)
            _ (when (neg? key-len) (throw (ex-info "bad key-len" {})))
            kb (byte-array key-len)
            _ (.get buf kb)
            val-len (.getInt buf)
            tomb? (= val-len -1)
            vb (when-not tomb?
                 (when (neg? val-len) (throw (ex-info "bad val-len" {})))
                 (let [a (byte-array val-len)] (.get buf a) a))
            stored-crc (.getInt buf)
            end (.position buf)
            payload (byte-array (- end start 4))]
        (.position buf start)
        (.get buf payload)
        (.position buf end)
        (let [crc (doto (CRC32.) (.update payload))]
          (when (= (unchecked-int (.getValue crc)) stored-crc)
            {:k (String. kb "UTF-8")
             :v (if tomb? ::tombstone (String. vb "UTF-8"))
             :next-pos end})))
      (catch java.nio.BufferUnderflowException _
        nil)
      (catch clojure.lang.ExceptionInfo _
        nil))))

(defn- replay
  "WALを先頭から詠み、[memtable good-bytes]を返す。"
  [^File file]
  (if-not (.exists file)
    [(sorted-map) 0]
    (let [buf (ByteBuffer/wrap (Files/readAllBytes (.toPath file)))]
      (loop [mt (sorted-map), good 0]
        (if-let [rec (try-read-record buf)]
          (recur (if (= (:v rec) ::tombstone)
                   (dissoc mt (:k rec))
                   (assoc mt (:k rec) (:v rec)))
                 (long (:next-pos rec)))
          [mt good])))))

(defn- truncate!
  [^File file ^long size]
  (with-open [raf (RandomAccessFile. file "rw")]
    (.setLength raf size)))

(defrecord WAL [^FileOutputStream out file])

(defn- wal-open [file]
  (->WAL (FileOutputStream. ^java.io.File file true) file))

(defn- wal-append! [^WAL wal ^bytes record]
  (let [^FileOutputStream out (:out wal)]
    (.write out record)
    (.sync (.getFD out))))

(defn- wal-close! [^WAL wal]
  (.close ^FileOutputStream (:out wal)))

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
      (println (format "[flush] memtable(%d) -> immutable / waiting=%d"
                       (count (:memtable old))
                       (count (:immutables new)))))))

(defrecord LSMStore [state wal opts]
  IKVStore
  (-put [this k v]
    (wal-append! wal (record-bytes k v))
    (swap! state update :memtable assoc k v)
    (maybe-flush! this)
    this)
  (-get [this k]
    (when-let [e (mem-find @state k)]
      (val e)))
  (-delete [this k]
    (wal-append! wal (record-bytes k :tombstone))
    (swap! state update :memtable dissoc k)
    this)
  (-close [this]
    (wal-close! wal)
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
           [mt good] (replay wal-file)]
       (when (< good (.length wal-file))
         (truncate! wal-file good))
       (->LSMStore (atom {:memtable mt :immutables []})
                   (wal-open wal-file)
                   opts)))))
