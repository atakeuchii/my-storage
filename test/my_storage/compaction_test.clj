(ns my-storage.compaction-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [my-storage.encoding :as enc]
            [my-storage.sstable :as sstable]
            [my-storage.compaction :as compaction]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsm" "")]
    (.delete d) (.mkdir d) (str d)))

(deftest merges-two-into-one-newest-wins
  (let [dir (temp-dir)
        f1 (sstable/next-sstable-file (io/file dir))
        _ (sstable/write-sstable! f1 (sorted-map "a" "1" "k" "old"))
        f2 (sstable/next-sstable-file (io/file dir))
        _ (sstable/write-sstable! f2 (sorted-map "k" "new" "z" "9"))
        r-new (sstable/open-reader f2)
        r-old (sstable/open-reader f1)
        out (compaction/compact! (io/file dir) [r-new r-old] false)
        r (sstable/open-reader out)]
    (is (= [["a" "1"] ["k" "new"] ["z" "9"]]
           (sstable/sstable-scan r nil nil)))
    (doseq [reader [r r-new r-old]]
      (sstable/close-reader! reader))))

(deftest drops-tombstones-when-flagged
  (let [dir (temp-dir)
        f1 (sstable/next-sstable-file (io/file dir))
        _ (sstable/write-sstable! f1 (sorted-map "a" "1" "b" "2"))
        f2 (sstable/next-sstable-file (io/file dir))
        _ (sstable/write-sstable! f2 (sorted-map "b" enc/tombstone))
        r-new (sstable/open-reader f2) r-old (sstable/open-reader f1)
        out (compaction/compact! (io/file dir) [r-new r-old] true)
        r (sstable/open-reader out)]
    (is (= [["a" "1"]] (sstable/sstable-scan r nil nil))) 
    (doseq [reader [r r-new r-old]]
      (sstable/close-reader! reader))))

(deftest pick-when-threshold-met
  (is (= [0 3] (compaction/pick-compaction [["a" 100] ["b" 100] ["c" 100] ["d" 100]] 4 1.5))))

(deftest pick-nil-when-too-few
  (is (nil? (compaction/pick-compaction [["a" 100] ["b" 100] ["c" 100]] 4 1.5))))

(deftest pick-groups-by-size
  (is (nil? (compaction/pick-compaction [["big" 1000] ["a" 100] ["b" 100] ["c" 100]] 4 1.5)))
  (is (= [1 4] (compaction/pick-compaction [["big" 1000] ["a" 100] ["b" 100] ["c" 100] ["d" 100]] 4 1.5))))
