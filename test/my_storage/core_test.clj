(ns my-storage.core-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
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
           (keys @(:data db))))
    (core/close db)))

(deftest wal-growson-put
  (let [dir (temp-dir)
        db (core/open dir)
        wal (io/file dir "wal.log")]
    (is (zero? (.length wal)))
    (core/put db "k" "v")
    (is (pos? (.length wal)))
    (core/close db)))
