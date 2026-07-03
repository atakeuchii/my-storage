(ns my-storage.core-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [my-storage.core :as core]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsm" "")]
    (.delete d)
    (.mkdir d)
    (str d)))

(deftest put-then-get
  (let [db (core/open (temp-dir))]
    (core/put db "k" "v")
    (is (= "v" (core/get db "k")))
    (core/close db)))

(deftest overwrite
  (let [db (core/open (temp-dir))]
    (core/put db "k" "v1")
    (core/put db "k" "v2")
    (is (= "v2" (core/get db "k")))
    (core/close db)))

(deftest missing-key
  (let [db (core/open (temp-dir))]
    (is (nil? (core/get db "nope")))
    (core/close db)))

(deftest delete-removes-key
  (let [db (core/open (temp-dir))]
    (core/put db "k" "v")
    (core/delete db "k")
    (is (nil? (core/get db "k")))
    (core/close db)))

(deftest keys-are-sorted
  (let [db (core/open (temp-dir))]
    (doseq [k ["c" "a" "b" "delta" "alpha"]]
      (core/put db k k))
    (is (= ["a" "alpha" "b" "c" "delta"]
           (keys (:memtable @(:state db)))))
    (core/close db)))

(deftest wal-growson-put
  (let [dir (temp-dir)
        db (core/open dir)
        wal (io/file dir "wal.log")]
    (is (zero? (.length wal)))
    (core/put db "k" "v")
    (is (pos? (.length wal)))
    (core/close db)))

(deftest wal-replay-recovers-data
  (let [dir (temp-dir)]
    (let [db (core/open dir)]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/delete db "a")
      (core/close db))
    (let [db2 (core/open dir)]
      (is (nil? (core/get db2 "a")))
      (is (= "2" (core/get db2 "b")))
      (core/close db2))))

(deftest wal-replay-survives-tail-corruption
  (let [dir (temp-dir)]
    (let [db (core/open dir)]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/close db))
    ;; 書き込み途中で落ちた状況を再現:keyLen=5 と宣言しつつ3バイトしか無いゴミを追記
    (let [wal (io/file dir "wal.log")]
      (with-open [o (java.io.FileOutputStream. wal true)]
        (.write o (byte-array [0 0 0 5 1 2 3]))))
    (let [db2 (core/open dir)]
      (is (= "1" (core/get db2 "a")))
      (is (= "2" (core/get db2 "b")))
      (core/put db2 "c" "3")
      (is (= "3" (core/get db2 "c")))
      (core/close db2))))

(deftest get-sees-immutable-after-flush
  (let [db (core/open (temp-dir) {:flush-threshold 3})]
    (core/put db "a" "1")
    (core/put db "b" "2")
    (core/put db "c" "3")
    (core/put db "d" "4")
    (is (= "4" (core/get db "d")))
    (core/close db)))

(deftest newst-wins-across-memtable-and-immutable
  (let [db (core/open (temp-dir) {:flush-threshold 3})]
    (core/put db "k" "old")
    (core/put db "x" "1")
    (core/put db "y" "2")
    (core/put db "k" "new")
    (is (= "new" (core/get db "k")))
    (core/close db)))

(deftest restart-collapses-immutables-into-memtable
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 3})]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/put db "c" "3")
      (core/put db "d" "4")
      (core/close db))
    (let [db2 (core/open dir {:flush-threshold 3})]
      (is (zero? (count (:immutables @(:state db2)))))
      (is (= "1" (core/get db2 "a")))
      (is (= "4" (core/get db2 "d")))
      (core/close db2))))

(deftest sstable-has-valid-footer
  (let [dir  (temp-dir)
        f    (io/file dir "t.db")
        _    (core/write-sstable! f (sorted-map "a" "1" "b" "2" "c" "3"))
        bs   (java.nio.file.Files/readAllBytes (.toPath f))
        len  (alength bs)
        buf  (doto (java.nio.ByteBuffer/wrap bs) (.position (- len 20)))
        idx  (.getLong buf)
        cnt  (.getInt buf)
        mgc  (let [m (byte-array 8)] (.get buf m) (String. m "UTF-8"))]
    (is (= 3 cnt))
    (is (= "MYSSTBL1" mgc))
    (is (< 0 idx len))))

(deftest flush-writes-sstable-file
  (let [dir (temp-dir)
        db  (core/open dir {:flush-threshold 3})]
    (core/put db "a" "1")
    (core/put db "b" "2")
    (core/put db "c" "3")
    (let [dbs (->> (.listFiles (io/file dir))
                   (filter #(str/ends-with? (.getName %) ".db")))]
      (is (= 1 (count dbs)))
      (is (zero? (count (:immutables @(:state db))))))
    (core/close db)))
