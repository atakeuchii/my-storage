(ns my-storage.wal
  "Write-Ahead Log の追記・リプレイ・切り詰め。"
  (:require [my-storage.encoding :as enc])
  (:import [java.io FileOutputStream File RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.file Files]))

(defrecord WAL [^FileOutputStream out file])

(defn open
  "追記モードで WAL を開く。"
  [file]
  (->WAL (FileOutputStream. ^File file true) file))

(defn append!
  "1レコードを追記し、fsync する。"
  [^WAL wal ^bytes record]
  (let [^FileOutputStream out (:out wal)]
    (.write out record)
    (.sync (.getFD out))))

(defn close!
  [^WAL wal]
  (.close ^FileOutputStream (:out wal)))

(defn replay
  "WAL を先頭から読み、[memtable good-bytes] を返す。
   good-bytes は壊れずに読めた末尾位置(= 健全なバイト数)。"
  [^File file]
  (if-not (.exists file)
    [(sorted-map) 0]
    (let [buf (ByteBuffer/wrap (Files/readAllBytes (.toPath file)))]
      (loop [mt (sorted-map), good 0]
        (if-let [rec (enc/try-read-record buf)]
          (recur (if (= (:v rec) enc/tombstone)
                   (dissoc mt (:k rec))
                   (assoc mt (:k rec) (:v rec)))
                 (long (:next-pos rec)))
          [mt good])))))

(defn truncate!
  "壊れた末尾を捨て、size バイトに切り詰める。"
  [^File file ^long size]
  (with-open [raf (RandomAccessFile. file "rw")]
    (.setLength raf size)))

(defn rotate! 
  [^WAL wal]
  (.close ^FileOutputStream (:out wal))
  (let [^File f (:file wal)]
    (with-open [raf (RandomAccessFile. f "rw")]
      (.setLength raf 0))
    (assoc wal :out (FileOutputStream. f true))))
