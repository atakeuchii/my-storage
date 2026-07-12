(ns my-storage.crash-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [my-storage.core :as core]
            [my-storage.sstable :as sstable]
            [my-storage.manifest :as mf])
  (:import [java.io File FileOutputStream]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsm" "")]
    (.delete d)
    (.mkdir d)
    (str d)))

(defn- db-files [dir]
  (->> (.listFiles (io/file dir))
       (filter #(.endsWith (.getName ^File %) ".db"))
       (map #(.getName ^File %))
       sort vec))

;; ── flush 途中: SSTable は書けたが、マニフェスト更新前に落ちた ──
;; 期待: SSTable は孤児として無視され、データは WAL から復元される。
(deftest crash-during-flush-before-manifest
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 1000})]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/put db "c" "3")
      (core/close db))
    (sstable/write-sstable! (sstable/next-sstable-file (io/file dir))
                            (sorted-map "a" "1" "b" "2" "c" "3"))
    (is (= 1 (count (db-files dir))))
    (is (empty? (:sstables (mf/load-manifest dir))))
    (let [db2 (core/open dir {:flush-threshold 1000})]
      (is (zero? (count (:sstables @(:state db2)))))
      (is (= "1" (core/get db2 "a")))
      (is (= "3" (core/get db2 "c")))
      (core/close db2))))

;; ── flush 途中: マニフェストは更新したが、WAL ローテート前に落ちた ──
;; 期待: SSTable と WAL の両方に同じデータ = 二重。newest-wins で吸収され、データは正しい。
(deftest crash-during-flush-before-wal-rotate
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 1000})]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/close db))
    (let [f (sstable/next-sstable-file (io/file dir))]
      (sstable/write-sstable! f (sorted-map "a" "1" "b" "2"))
      (mf/add-sstable! dir (.getName f)))
    (is (pos? (.length (io/file dir "wal.log"))))
    (let [db2 (core/open dir {:flush-threshold 1000})]
      (is (= 1 (count (:sstables @(:state db2)))) )
      (is (= 2 (count (:memtable @(:state db2)))))
      (is (= "1" (core/get db2 "a")))
      (is (= "2" (core/get db2 "b")))
      (is (= [["a" "1"] ["b" "2"]] (vec (core/scan db2 nil nil))))
      (core/close db2))))

;; ── compaction 途中: 新SSTableは書けたが、マニフェスト差し替え前に落ちた ──
;; 期待: 古いSSTable群がまだ有効。新ファイルは孤児として無視される。
(deftest crash-during-compaction-before-manifest-swap
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 2})]
      (doseq [i (range 4)]
        (core/put db (format "k%d" i) (str i)))
      (core/close db))
    (is (= 2 (count (:sstables (mf/load-manifest dir)))))
    (sstable/write-sstable! (sstable/next-sstable-file (io/file dir))
                            (sorted-map "k0" "0" "k1" "1" "k2" "2" "k3" "3"))
    (is (= 3 (count (db-files dir))))
    (let [db2 (core/open dir {:flush-threshold 2})]
      (is (= 2 (count (:sstables @(:state db2)))))
      (is (every? #(= (str %) (core/get db2 (format "k%d" %))) (range 4)))
      (core/close db2))))

;; ── compaction 途中: マニフェストは差し替えたが、古いファイル削除前に落ちた ──
;; 期待: 新1枚が有効。古いファイルは孤児として無視される(ディスクに残るだけ)。
(deftest crash-during-compaction-before-delete
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 2})]
      (doseq [i (range 4)]
        (core/put db (format "k%d" i) (str i)))
      (core/close db))
    (let [merged (sstable/next-sstable-file (io/file dir))]
      (is (= 2 (count (:sstables (mf/load-manifest dir)))))
      (sstable/write-sstable! merged (sorted-map "k0" "0" "k1" "1" "k2" "2" "k3" "3"))
      (mf/save-manifest! dir {:sstables [(.getName merged)]})
      (is (= 3 (count (db-files dir))))
      (is (= 1 (count (:sstables (mf/load-manifest dir))))))
    (let [db2 (core/open dir {:flush-threshold 2})]
      (is (= 1 (count (:sstables @(:state db2)))))
      (is (every? #(= (str %) (core/get db2 (format "k%d" %))) (range 4)))
      (core/close db2))))

;; ── SSTable の書き込み途中で落ちた(壊れた .db ファイルが残る) ──
;; 期待: マニフェストに載っていないので一度も開かれない。起動は成功する。
(deftest crash-mid-sstable-write-leaves-corrupt-file
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 1000})]
      (core/put db "a" "1")
      (core/close db))
    (with-open [out (FileOutputStream. (io/file dir "sstable-9999999999999-999999999.db"))]
      (.write out (byte-array [0 0 0 1 97 0 0 0 1 49])))
    (let [db2 (core/open dir {:flush-threshold 1000})]
      (is (= "1" (core/get db2 "a")))
      (is (zero? (count (:sstables @(:state db2)))))
      (core/close db2))))

;; ── 削除が compaction 途中クラッシュを跨いでも resurrect しない ──
(deftest delete-survives-compaction-crash
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 2})]
      (core/put db "x" "old")
      (core/put db "y" "1")
      (core/delete db "x")
      (core/put db "z" "2")
      (core/close db))
    (let [db (core/open dir {:flush-threshold 2})]
      (is (nil? (core/get db "x")))
      (core/close db))
    (sstable/write-sstable! (sstable/next-sstable-file (io/file dir))
                            (sorted-map "y" "1" "z" "2"))
    (let [db2 (core/open dir {:flush-threshold 2})]
      (is (nil? (core/get db2 "x")))
      (is (= "1" (core/get db2 "y")))
      (core/close db2))))
