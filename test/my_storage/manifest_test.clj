(ns my-storage.manifest-test
  (:require [clojure.test :refer [deftest is]]
            [my-storage.manifest :as mf]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsm" "")]
    (.delete d) (.mkdir d) (str d)))

(deftest load-empty-when-absent
  (is (= {:sstables []} (mf/load-manifest (temp-dir)))))

(deftest save-load-roundtrip
  (let [dir (temp-dir)
        m {:sstables ["a.db" ".db"]}]
    (mf/save-manifest! dir m)
    (is (= m (mf/load-manifest dir)))))

(deftest add-appends-in-order
  (let [dir (temp-dir)]
    (mf/add-sstable! dir "s1.db")
    (mf/add-sstable! dir "s2.db")
    (mf/add-sstable! dir "s3.db")
    (is (= ["s1.db" "s2.db" "s3.db"] (:sstables (mf/load-manifest dir))))))
