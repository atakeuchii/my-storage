(ns my-storage.wal-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [my-storage.core :as core]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsm" "")]
    (.delete d)
    (.mkdir d)
    (str d)))

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
