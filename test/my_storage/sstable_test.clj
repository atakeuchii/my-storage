(ns my-storage.sstable-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [my-storage.sstable :as sstable]
            [my-storage.encoding :as enc]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsm" "")]
    (.delete d)
    (.mkdir d)
    (str d)))

(deftest sstable-has-valid-footer
  (let [dir  (temp-dir)
        f    (io/file dir "t.db")
        _    (sstable/write-sstable! f (sorted-map "a" "1" "b" "2" "c" "3"))
        bs   (java.nio.file.Files/readAllBytes (.toPath f))
        len  (alength bs)
        buf  (doto (java.nio.ByteBuffer/wrap bs) (.position (- len 20)))
        idx  (.getLong buf)
        cnt  (.getInt buf)
        mgc  (let [m (byte-array 8)] (.get buf m) (String. m "UTF-8"))]
    (is (= 3 cnt))
    (is (= "MYSSTBL1" mgc))
    (is (< 0 idx len))))

(deftest reader-point-lookup
  (let [dir (temp-dir)
        f (io/file dir "t.db")
        _ (sstable/write-sstable! f (sorted-map "apple" "A" "banana" "B" "cherry" "C"))
        r (sstable/open-reader f)]
    (is (= "A" (sstable/sstable-get r "apple")))
    (is (= "B" (sstable/sstable-get r "banana")))
    (is (= "C" (sstable/sstable-get r "cherry")))
    (sstable/close-reader! r)))

(deftest reader-missing-key
  (let [dir (temp-dir)
        f (io/file dir "t.db")
        _ (sstable/write-sstable! f (sorted-map "b" "1" "d" "3" "f" "5"))
        r (sstable/open-reader f)]
    (is (= enc/not-found (sstable/sstable-get r "a")))
    (is (= enc/not-found (sstable/sstable-get r "c")))
    (is (= enc/not-found (sstable/sstable-get r "e")))
    (sstable/close-reader! r)))

(deftest reader-tombstone
  (let [dir (temp-dir)
        f (io/file dir "t.db")
        _ (sstable/write-sstable! f (sorted-map "a" "1" "b" enc/tombstone "c" "3"))
        r (sstable/open-reader f)]
    (is (= "1" (sstable/sstable-get r "a")))
    (is (= enc/tombstone (sstable/sstable-get r "b")))
    (is (= "3" (sstable/sstable-get r "c")))
    (sstable/close-reader! r)))

(deftest readr-manyentries-across-ndex-blocks
  (let [dir (temp-dir)
        f (io/file dir "t.db")
        entries (into (sorted-map)
                      (for [i (range 1000)]
                        [(format "key%04d" i) (str "val" i)]))
        _ (sstable/write-sstable! f entries)
        r (sstable/open-reader f)]
    (is (every? (fn [i] (= (str "val" i)
                           (sstable/sstable-get r (format "key%04d" i))))
                (range 1000)))
    (is (= enc/not-found (sstable/sstable-get r "key9999")))
    (sstable/close-reader! r)))
