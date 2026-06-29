(ns my-storage.core
  (:refer-clojure :exclude [get])
  (:require [clojure.java.io :as io])
  (:import [java.io FileOutputStream ByteArrayOutputStream DataOutputStream]
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

(defrecord WAL [^FileOutputStream out file])

(defn- wal-open [file]
  (->WAL (FileOutputStream. ^java.io.File file true) file))

(defn- wal-append! [^WAL wal ^bytes record]
  (let [^FileOutputStream out (:out wal)]
    (.write out record)
    (.sync (.getFD out))))

(defn- wal-close! [^WAL wal]
  (.close ^FileOutputStream (:out wal)))

(defrecord LSMStore [data wal]
  IKVStore
  (-put [this k v]
    (wal-append! wal (record-bytes k v))
    (swap! data assoc k v)
    this)
  (-get [this k]
    (clojure.core/get @data k))
  (-delete [this k]
    (wal-append! wal (record-bytes k :tombstone))
    (swap! data dissoc k)
    this)
  (-close [this] 
    (wal-close! wal)
    nil))

(defn put [store k v] (-put store k v))
(defn get [store k] (-get store k))
(defn delete [store k] (-delete store k))
(defn close [store] (-close store))

(defn open
  [dir]
  (let [d (io/file dir)]
    (.mkdirs d)
    (->LSMStore (atom (sorted-map))
                (wal-open (io/file d "wal.log")))))
