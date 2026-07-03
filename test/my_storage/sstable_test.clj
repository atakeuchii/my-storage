(ns my-storage.sstable-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [my-storage.sstable :as sstable]))

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
