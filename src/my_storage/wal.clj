(ns my-storage.wal
  "Write-Ahead Log の追記・リプレイ・切り詰め。"
  (:require [my-storage.encoding :as enc])
  (:import [java.io FileOutputStream BufferedOutputStream File RandomAccessFile]
           [java.nio ByteBuffer]
           [java.nio.file Files]))

(defrecord WAL [^FileOutputStream out ^BufferedOutputStream buf file fsync-policy pending])

(defn open
  "追記モードで WAL を開く。"
  ([file] (open file {}))
  ([file opts]
   (let [fos (FileOutputStream. ^File file true)
         bos (BufferedOutputStream. fos 65536)]
     (->WAL fos bos file (:wal-fsync opts :always) (atom 0)))))

(defn fsync! [^WAL wal]
  (.flush ^BufferedOutputStream (:buf wal))
  (.sync (.getFD ^FileOutputStream (:out wal))))

(defn append!
  "1レコードを追記し、fsync する。"
  [^WAL wal ^bytes record]
  (.write ^BufferedOutputStream (:buf wal) record)
  (let [policy (:fsync-policy wal)]
    (cond
      (= policy :always) (fsync! wal)
      (= policy :never) nil
      (integer? policy) (when (zero? (mod (swap! (:pending wal) inc) (long policy)))
                          (fsync! wal))))
  wal)

(defn close!
  [^WAL wal]
  (fsync! wal)
  (.close ^BufferedOutputStream (:buf wal)))

(defn replay
  "WAL を先頭から読み、[memtable good-bytes] を返す。
   good-bytes は壊れずに読めた末尾位置(= 健全なバイト数)。"
  [^File file]
  (if-not (.exists file)
    [(sorted-map) 0]
    (let [buf (ByteBuffer/wrap (Files/readAllBytes (.toPath file)))]
      (loop [mt (sorted-map)
             good 0]
        (if-let [rec (enc/try-read-record buf)]
          (recur (assoc mt (:k rec) (:v rec))
                 (long (:next-pos rec)))
          [mt good])))))

(defn truncate!
  "壊れた末尾を捨て、size バイトに切り詰める。"
  [^File file ^long size]
  (with-open [raf (RandomAccessFile. file "rw")]
    (.setLength raf size)))

(defn rotate! 
  [^WAL wal]
  (.flush ^BufferedOutputStream (:buf wal))
  (.close ^BufferedOutputStream (:buf wal))
  (let [^File f (:file wal)]
    (with-open [raf (RandomAccessFile. f "rw")]
      (.setLength raf 0))
    (let [fos (FileOutputStream. f true)
          bos (BufferedOutputStream. fos 6556)]
      (assoc wal :out fos :buf bos :pending (atom 0)))))
