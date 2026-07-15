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
        buf  (doto (java.nio.ByteBuffer/wrap bs) (.position (- len 28)))
        bloom-off  (.getLong buf)
        idx  (.getLong buf)
        cnt  (.getInt buf)
        mgc  (let [m (byte-array 8)] (.get buf m) (String. m "UTF-8"))]
    (is (= 3 cnt))
    (is (= "MYSSTBL1" mgc))
    (is (< 0 idx bloom-off len))))

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

(deftest readr-many-entries-across-ndex-blocks
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

(deftest bloom-no-false-negative-in-sstable
  (let [dir (temp-dir)
        f (io/file dir "t.db")
        entrie (into (sorted-map)
                     (for [i (range 300)] [(format "k%04d" i) (str i)]))
        _ (sstable/write-sstable! f entrie)
        r (sstable/open-reader f)]
    (is (every? #(sstable/might-contain? r (format "k%04d" %)) (range 300)))
    (sstable/close-reader! r)))

(deftest sstable-scan-range
  (let [dir (temp-dir)
        f (io/file dir "t.db")
        _ (sstable/write-sstable!
           f (into (sorted-map) (for [c "abcdefg"] [(str c) (str c)])))
        r (sstable/open-reader f)]
    (is (= [["b" "b"] ["c" "c"] ["d" "d"]]
           (sstable/sstable-scan r "b" "e")))
    (is (= [["a" "a"] ["b" "b"] ["c" "c"] ["d" "d"] ["e" "e"] ["f" "f"] ["g" "g"]]
           (sstable/sstable-scan r nil nil)))
    (is (= [["e" "e"] ["f" "f"] ["g" "g"]]
           (sstable/sstable-scan r "e" nil)))
    (sstable/close-reader! r)))

(deftest sstable-scan-desc-reads-in-reverse
  (let [dir (temp-dir)
        f   (io/file dir "d.db")
        _   (sstable/write-sstable!
             f (into (sorted-map)
                     (map (fn [i] [(format "k%03d" i) (str i)]) (range 100)))
             {:index-interval 8})
        r   (sstable/open-reader f)]
    (is (= (map #(format "k%03d" %) (reverse (range 100)))
           (map first (sstable/sstable-scan-desc r nil nil))))
    (is (= (map #(format "k%03d" %) (reverse (range 20 30)))
           (map first (sstable/sstable-scan-desc r "k020" "k030"))))
    (is (= (reverse (sstable/sstable-scan r nil nil))
           (sstable/sstable-scan-desc r nil nil)))
    (sstable/close-reader! r)))
