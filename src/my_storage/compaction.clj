(ns my-storage.compaction
  (:require [my-storage.encoding :as enc]
            [my-storage.sstable :as sstable]
            [my-storage.merge :as merge]))

(defn compact!
  ([dir readers drop-tombstones?] (compact! dir readers drop-tombstones? {}))
  ([dir readers drop-tombstones? sst-opts]
   (let [sources (map #(sstable/sstable-scan % nil nil) readers)
         merged (merge/merge-sorted sources)
         entries (vec (if drop-tombstones?
                        (remove (fn [[_ v]] (= v enc/tombstone)) merged)
                        merged))
         file (sstable/next-sstable-file dir)]
     (sstable/write-sstable! file entries sst-opts)
     file)))

(defn pick-compaction
  [sstable-sizes threshold size-ratio]
  (let [n (count sstable-sizes)]
    (loop [i 0]
      (when (< i n)
        (let [base (second (nth sstable-sizes i))
              j (loop [j i]
                  (if (and (< (inc j) n)
                           (let [s (second (nth sstable-sizes (inc j)))
                                 hi (double (max s base))
                                 lo (double (max i (min s base)))]
                             (< (/ hi lo) size-ratio)))
                    (recur (inc j))
                    j))]
          (if (>= (- (inc j) i) threshold)
            [i j]
            (recur (inc j))))))))
