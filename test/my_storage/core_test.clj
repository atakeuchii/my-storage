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

(deftest get-from-sstable-after-flush
  (let [db (core/open (temp-dir) {:flush-threshold 3})]
    (core/put db "a" "1")
    (core/put db "b" "2")
    (core/put db "c" "3")
    (is (= {} (:memtable @(:state db))))
    (is (= "1" (core/get db "a")))
    (is (= "2" (core/get db "b")))
    (is (= "3" (core/get db "c")))
    (core/close db)))

(deftest newest-wins-across-sstables
  (let [db (core/open (temp-dir) {:flush-threshold 2})]
    (core/put db "k" "old")
    (core/put db "x" "1")
    (core/put db "y" "2")
    (core/put db "k" "new")
    (is (= "new" (core/get db "k")))
    (is (= "1" (core/get db "x")))
    (is (= "2" (core/get db "y")))
    (is (nil? (core/get db "nope")))
    (core/close db)))

(deftest reopen-preserves-values
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 3})]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/put db "c" "3")
      (core/put db "d" "4")
      (core/close db))
    (let [db2 (core/open dir {:flush-threshold 3})]
      (is (= "1" (core/get db2 "a")))
      (is (= "2" (core/get db2 "b")))
      (is (= "3" (core/get db2 "c")))
      (core/close db2))))

(deftest bloom-kips-sstable-reads-for-absent-key
  (let [db (core/open (temp-dir) {:flush-threshold 2})]
    (doseq [i (range 10)]
      (core/put db (format "k%02d" i) (str i)))
    (core/get db "zzz-absent")
    (let [{:keys [reads skips]} @(:stats db)]
      (is (pos? skips))
      (is (> skips reads) (str "reads=" reads " skips=" skips)))
    (core/close db)))

(deftest bloom-does-not-hide-present-key
  (let [db (core/open (temp-dir) {:flush-threshold 2})]
    (doseq [i (range 10)]
      (core/put db (format "k%02d" i) (str i)))
    (is (every? #(= (str %) (core/get db (format "k%02d" %))) (range 10)))
    (core/close db)))
