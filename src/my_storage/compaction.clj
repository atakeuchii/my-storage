(ns my-storage.compaction
  (:require [my-storage.encoding :as enc]
            [my-storage.sstable :as sstable]
            [my-storage.merge :as merge]))

(defn compact!
  [dir readers drop-tombstones?]
  (let [sources (map #(sstable/sstable-scan % nil nil) readers)
        merged (merge/merge-sorted sources)
        entries (vec (if drop-tombstones?
                       (remove (fn [[_ v]] (= v enc/tombstone)) merged)
                       merged))
        file (sstable/next-sstable-file dir)]
    (sstable/write-sstable! file entries)
    file))
