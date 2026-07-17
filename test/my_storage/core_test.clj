(ns my-storage.core-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [my-storage.core :as core]
            [my-storage.table :as tbl]
            [my-storage.manifest :as mf]))

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

(deftest wal-is-short-after-flush
  (let [dir (temp-dir)
        db (core/open dir {:flush-threshold 3})
        wal (io/file dir "wal.log")]
    (core/put db "a" "1")
    (core/put db "b" "2")
    (core/put db "c" "3")
    (is (zero? (.length wal)))
    (core/put db "d" "4")
    (is (pos? (.length wal)))
    (core/close db)))

(deftest manifest-records-sstables
  (let [dir (temp-dir)
        db (core/open dir {:flush-threshold 2})]
    (doseq [i (range 6)]
      (core/put db (format "k%d" i) (str i)))
    (is (= 3 (count (:sstables (mf/load-manifest dir)))))
    (core/close db)))

(deftest restart-recovers-from-manifest-and-wal
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 3})]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/put db "c" "3")
      (core/put db "d" "4")
      (core/close db))
    (let [db2 (core/open dir {:flush-threshold 3})]
      (is (= "1" (core/get db2 "a")))
      (is (= "4" (core/get db2 "d")))
      (is (= 1 (count (:memtable @(:state db2)))))
      (is (= 1 (count (:sstables @(:state db2)))))
      (core/close db2))))

(deftest cross-sstable-consistency-after-restart
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 2})]
      (doseq [i (range 20)]
        (core/put db (format "key%02d" i) (str i)))
      (core/close db))
    (let [db2 (core/open dir {:flush-threshold 2})]
      (is (every? #(= (str %) (core/get db2 (format "key%02d" %))) (range 20)))
      (core/close db2))))

(deftest delete-works-for-flushed-key
  (let [db (core/open (temp-dir) {:flush-threshold 3})]
    (core/put db "a" "1")
    (core/put db "b" "2")
    (core/put db "c" "3")
    (is (= "1" (core/get db "a")))
    (core/delete db "a")
    (is (nil? (core/get db "a")))
    (is (= "2" (core/get db "b")))
    (core/close db)))

(deftest reput-after-delete
  (let [db (core/open (temp-dir) {:flush-threshold 3})]
    (core/put db "a" "1")
    (core/put db "b" "2")
    (core/put db "c" "3")
    (is (= "1" (core/get db "a")))
    (core/delete db "a")
    (is (nil? (core/get db "a")))
    (core/put db "a" "1b")
    (is (= "1b" (core/get db "a")))
    (core/close db)))

(deftest delete-survives-restart
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 3})]
      (core/put db "a" "1")
      (core/put db "b" "2")
      (core/put db "c" "3")
      (core/delete db "a")
      (core/close db))
    (let [db2 (core/open dir {:flush-threshold 3})]
      (is (nil? (core/get db2 "a")))
      (is (= "2" (core/get db2 "b")))
      (core/close db2))))

(deftest scan-sorted-across-layers
  (let [db (core/open (temp-dir) {:flush-threshold 3})]
    (core/put db "c" "3")
    (core/put db "a" "1")
    (core/put db "e" "5")
    (core/put db "b" "2")
    (core/put db "d" "4")
    (core/put db "a" "1b")
    (core/delete db "c")
    (is (= [["a" "1b"] ["b" "2"] ["d" "4"] ["e" "5"]]
           (core/scan db "a" "z")))
    (is (= [["b" "2"] ["d" "4"]]
           (core/scan db "b" "e")))
    (core/close db)))

(deftest compact-reduces-to-one-file-preserving-data
  (let [dir (temp-dir)
        db (core/open dir {:flush-threshold 2})]
    (doseq [i (range 10)]
      (core/put db (format "k%02d" i) (str i)))
    (is (= 5 (count (:sstables @(:state db)))))
    (core/compact! db)
    (is (= 1 (count (:sstables @(:state db)))))
    (is (every? #(= (str %) (core/get db (format "k%02d" %))) (range 10)))
    (core/close db)))

(deftest compact-reclaims-space-from-deletes
  (let [dir (temp-dir)
        db (core/open dir {:flush-threshold 2})]
    (doseq [i (range 20)]
      (core/put db "hot" (str i)))
    (doseq [i (range 10)]
      (core/put db (format "k%d" i) (str i)))
    (doseq [i (range 10)]
      (core/delete db (format "k%d" i)))
    (let [before (->> (:sstables @(:state db)) (map #(.length (:file %))) (reduce +))]
      (core/compact! db)
      (let [after (->> (:sstables @(:state db)) (map #(.length (:file %))) (reduce +))]
        (is (< after before) (str "before=" before " after=" after))
        (is (nil? (core/get db "k0")))
        (is (= "19" (core/get db "hot")))
        (is (= [["hot" "19"]] (core/scan db nil nil)))))
    (core/close db)))

(deftest compact-survives-restart
  (let [dir (temp-dir)]
    (let [db (core/open dir {:flush-threshold 2})]
      (doseq [i (range 8)] (core/put db (format "k%d" i) (str i)))
      (core/compact! db)
      (core/close db))
    (let [db2 (core/open dir {:flush-threshold 2})]
      (is (= 1 (count (:sstables @(:state db2)))))
      (is (every? #(= (str %) (core/get db2 (format "k%d" %))) (range 8)))
      (core/close db2))))

;; Day11: 大量 put で SSTable が増えても、自動コンパクションで枚数が抑えられる
(deftest auto-compaction-bounds-sstable-count
  (let [db (core/open (temp-dir) {:flush-threshold 2 :compaction-threshold 4})]
    (doseq [i (range 100)]
      (core/put db (format "k%03d" i) (str i)))
    (is (< (count (:sstables @(:state db))) 20)
        (str "sstables=" (count (:sstables @(:state db)))))
    (is (every? #(= (str %) (core/get db (format "k%03d" %))) (range 100)))
    (core/close db)))

;; Day11: 部分マージ(最古を含まない)では tombstone を保持し、削除が resurrect しない
(deftest partial-compaction-keeps-tombstone-correct
  (let [dir (temp-dir)
        db (core/open dir {:flush-threshold 2 :compaction-threshold 4})]
    (core/put db "x" "old")
    (core/put db "p0" "0")
    (core/delete db "x")
    (doseq [i (range 40)]
      (core/put db (format "k%02d" i) (str i)))
    (is (nil? (core/get db "x")))
    (core/close db)
    (let [db2 (core/open dir {:flush-threshold 2 :compaction-threshold 4})]
      (is (nil? (core/get db2 "x")))
      (is (every? #(= (str %) (core/get db2 (format "k%02d" %))) (range 40)))
      (core/close db2))))

;; Day11: 自動コンパクション後も scan が一貫している
(deftest scan-consistent-after-auto-compaction
  (let [db (core/open (temp-dir) {:flush-threshold 2 :compaction-threshold 4})]
    (doseq [i (range 30)]
      (core/put db (format "k%02d" i) (str i)))
    (doseq [i (range 0 30 2)]
      (core/delete db (format "k%02d" i)))
    (let [result (core/scan db nil nil)
          expected (for [i (range 30) :when (odd? i)] [(format "k%02d" i) (str i)])]
      (is (= (vec expected) (vec result))))
    (core/close db)))

(deftest rscan-is-reverse-of-scan
  (let [db (core/open (temp-dir) {:flush-threshold 10 :compaction-threshold 2})]
    (doseq [i (range 235)] (core/put db (format "k%04d" i) (str "v" i)))
    (core/put db "k0100" "OVER")
    (core/delete db "k0050")
    (is (= (reverse (core/scan db nil nil))
           (into [] (core/rscan db nil nil))))
    (core/close db)))

(deftest rscan-take-n-is-latest-n
  (let [db (core/open (temp-dir) {:flush-threshold 50})]
    (doseq [i (range 1000)] (core/put db (format "k%05d" i) (str i)))
    (is (= ["k00999" "k00998" "k00997"]
           (map first (take 3 (core/rscan db nil nil)))))
    (core/close db)))

(deftest table-find-by-index-two-hop
  (let [db (core/open (temp-dir) {})]
    (tbl/insert-row! db "users" :id [:email :age] {:id 1 :name "a" :email "a@x" :age 30})
    (tbl/insert-row! db "users" :id [:email :age] {:id 2 :name "b" :email "b@x" :age 30})
    (is (= {:id 1 :name "a" :email "a@x" :age 30}
           (first (tbl/find-by-index db "users" :email "a@x"))))
    (is (= #{1 2} (set (map :id (tbl/find-by-index db "users" :age 30)))))
    (core/close db)))

(deftest table-range-by-index-respects-order
  (let [db (core/open (temp-dir) {})]
    (doseq [[id age] [[1 5] [2 8] [3 25] [4 30] [5 100]]]
      (tbl/insert-row! db "users" :id [:age] {:id id :age age}))
    (is (= [8 25] (map :age (tbl/range-by-index db "users" :age 6 26))))
    (core/close db)))

(deftest table-update-leaves-no-ghost-index
  (let [db (core/open (temp-dir) {})]
    (tbl/insert-row! db "users" :id [:age] {:id 1 :name "a" :age 30})
    (tbl/update-row! db "users" :id [:age] {:id 1 :name "a" :age 31})
    (is (empty? (tbl/find-by-index db "users" :age 30)))
    (is (= [31] (map :age (tbl/find-by-index db "users" :age 31))))
    (core/close db)))

(deftest concurrent-writes-survive-restart
  ;; 複数スレッドから同時に put しても、例外なく全件が耐久化される。
  (let [dir (temp-dir) n-thr 8 n-each 500 total (* n-thr n-each)
        db (core/open dir {:flush-threshold 100 :wal-fsync :always})
        errors (atom 0)]
    (->> (for [t (range n-thr)]
           (future (dotimes [i n-each]
                     (try (core/put db (format "k%02d-%05d" t i) (str t "-" i))
                          (catch Throwable _ (swap! errors inc))))))
         (map deref) doall)
    (is (zero? @errors) "並行 put で例外が出ないこと")
    (is (= total (count (core/scan db nil nil))) "close 前に全件見えること")
    (core/close db)
    (let [db2 (core/open dir {:flush-threshold 100 :wal-fsync :always})]
      (is (= total (count (core/scan db2 nil nil))) "再起動後も全件生存すること")
      (core/close db2))))
