(ns my-storage.scale
  "テーマ1: 規模を入れて壊れ方を見る。
   Day13 が『定常状態のスナップショット』だったのに対し、ここでは
   『時間とともに何が起きるか』(増幅・ポーズ・メモリ)を測る。"
  (:require [my-storage.core :as lsm])
  (:import [java.io File]))

;; ── ユーティリティ ──────────────────────────────

(defn- rm-rf [^File f]
  (when (.isDirectory f) (doseq [c (.listFiles f)] (rm-rf c)))
  (.delete f))

(defn- pct [sorted-longs p]
  (let [n (count sorted-longs)]
    (if (zero? n) 0 (nth sorted-longs (min (dec n) (long (* p n)))))))

(defn- heap-mb []
  (let [rt (Runtime/getRuntime)]
    (/ (- (.totalMemory rt) (.freeMemory rt)) 1048576.0)))

(defn- gc! [] (System/gc) (Thread/sleep 200))

(defn- dir-bytes [dir]
  (->> (.listFiles (File. ^String dir))
       (filter #(.isFile ^File %))
       (map #(.length ^File %))
       (reduce + 0)))

(defn- sstable-sizes [db]
  (->> (:sstables @(:state db))
       (mapv #(.length ^File (:file %)))))

;; ── 本体 ──────────────────────────────

(defn run
  "n 件を put しながら、スループット推移・レイテンシ分布・増幅・ポーズ・ヒープを計測する。
   val-size: 値のバイト数(大きくすると本物のディスクI/Oに近づく)。"
  [{:keys [n val-size opts chunk]
    :or   {n 1000000 val-size 100 chunk 100000}}]
  (let [dir (str (doto (File/createTempFile "lsmscale" "") (.delete) (.mkdir)))
        db  (lsm/open dir opts)
        v   (apply str (repeat val-size \x))
        key-of (fn [i] (format "key%08d" i))
        logical (* n (+ 11 val-size))]      ; おおよその論理データ量(key11B + value)
    (println (format "\n=== n=%,d / value=%dB / opts=%s ===" n val-size (pr-str opts)))
    (println "chunk        tps      経過(s)  SSTable枚数  ディスク(MB)  ヒープ(MB)")
    (try
      (let [lat (long-array n)              ; 各 put のレイテンシ(ns)
            t-start (System/nanoTime)]
        (dotimes [i n]
          (let [t (System/nanoTime)]
            (lsm/put db (key-of i) v)
            (aset lat i (- (System/nanoTime) t)))
          ;; chunk ごとに進捗を出す(スループットの劣化が見える)
          (when (zero? (mod (inc i) chunk))
            (let [done (inc i)
                  elapsed (/ (- (System/nanoTime) t-start) 1e9)]
              (println (format "%,9d  %,8d  %7.1f  %10d  %11.1f  %9.1f"
                               done
                               (long (/ done elapsed))
                               elapsed
                               (count (:sstables @(:state db)))
                               (/ (dir-bytes dir) 1048576.0)
                               (heap-mb))))))

        (let [total-s (/ (- (System/nanoTime) t-start) 1e9)
              sorted  (vec (sort (seq lat)))
              st      (lsm/stats db)
              disk    (dir-bytes dir)
              written (+ (:wal-bytes st) (:sstable-bytes st))
              cms     (:compaction-ms st)]

          ;; ── 書き込みレイテンシ分布(コンパクションのポーズが見える)──
          (println "\n--- put レイテンシ分布 ---")
          (doseq [[label p] [["p50" 0.50] ["p99" 0.99] ["p99.9" 0.999] ["p99.99" 0.9999]]]
            (println (format "  %-7s %10.1f us" label (/ (pct sorted p) 1000.0))))
          (println (format "  %-7s %10.1f us   ← 最悪の1件(コンパクションのポーズ)"
                           "max" (/ (double (last sorted)) 1000.0)))

          ;; ── 増幅 ──
          (println "\n--- 増幅(amplification) ---")
          (println (format "  論理データ量        : %8.1f MB" (/ logical 1048576.0)))
          (println (format "  WAL 書き込み        : %8.1f MB" (/ (:wal-bytes st) 1048576.0)))
          (println (format "  SSTable 書き込み    : %8.1f MB  (うち compaction %.1f MB)"
                           (/ (:sstable-bytes st) 1048576.0)
                           (/ (:compaction-bytes st) 1048576.0)))
          (println (format "  書き込み増幅        : %8.2f 倍  ← 1バイト書くとディスクに何バイト書かれたか"
                           (double (/ written logical))))
          (println (format "  ディスク上のサイズ  : %8.1f MB" (/ disk 1048576.0)))
          (println (format "  空間増幅            : %8.2f 倍" (double (/ disk logical))))

          ;; ── コンパクション ──
          (println "\n--- コンパクション ---")
          (println (format "  flush 回数          : %d" (:flushes st)))
          (println (format "  compaction 回数     : %d" (count cms)))
          (when (seq cms)
            (println (format "  合計時間            : %8.1f ms (全体の %.1f%%)"
                             (reduce + cms)
                             (* 100.0 (/ (reduce + cms) (* total-s 1000)))))
            (println (format "  最長の1回           : %8.1f ms  ← write stall の正体"
                             (apply max cms))))

          ;; ── tier 構造 ──
          (println "\n--- SSTable の tier 構造(新→古, MB)---")
          (println (format "  %s"
                           (pr-str (mapv #(Double/parseDouble (format "%.1f" (/ % 1048576.0)))
                                         (sstable-sizes db)))))

          ;; ── 読み込み ──
          (gc!)
          (println (format "\n--- 読み込み(SSTable %d枚)---" (count (:sstables @(:state db)))))
          (lsm/reset-stats! db)
          (let [rng (java.util.Random. 42)
                reads 2000
                rl (vec (sort (for [_ (range reads)]
                                (let [i (.nextInt rng n) t (System/nanoTime)]
                                  (lsm/get db (key-of i))
                                  (- (System/nanoTime) t)))))
                rst (lsm/stats db)]
            (println (format "  get p50 = %6.1f us / p99 = %7.1f us / read-amp = %.2f"
                             (/ (pct rl 0.50) 1000.0) (/ (pct rl 0.99) 1000.0)
                             (:read-amp rst))))
          (lsm/reset-stats! db)
          (let [mid (quot n 2)
                t (System/nanoTime)
                cnt (count (lsm/scan db (key-of mid) (key-of (+ mid 1000))))]
            (println (format "  scan 1000件 = %.1f ms (取得 %d 件)"
                             (/ (- (System/nanoTime) t) 1e6) cnt)))

          ;; ── ヒープ(既知の制約: compaction が全実体化)──
          (println "\n--- メモリ ---")
          (gc!)
          (println (format "  安定時ヒープ        : %8.1f MB" (heap-mb)))
          (let [before (heap-mb)
                t (System/nanoTime)]
            (lsm/compact! db)                  ; 全 SSTable を1枚にする最重量マージ
            (println (format "  フルcompaction      : %8.1f ms / ヒープ %.1f → %.1f MB  ← 既知の OOM リスク"
                             (/ (- (System/nanoTime) t) 1e6) before (heap-mb))))
          (println (format "  compaction 後のディスク: %.1f MB / SSTable %d枚"
                           (/ (dir-bytes dir) 1048576.0)
                           (count (:sstables @(:state db)))))))
      (finally
        (lsm/close db)
        (rm-rf (File. dir))))))

(defn -main [& args]
  (let [n (if (first args) (Long/parseLong (first args)) 1000000)
        vs (if (second args) (Long/parseLong (second args)) 100)]
    (run {:n n
          :val-size vs
          :chunk (max 1 (quot n 10))
          :opts {:flush-threshold 50000
                 :compaction-threshold 4
                 :wal-fsync 1000}})       ; fsync が支配しないよう緩める
    (shutdown-agents)))
