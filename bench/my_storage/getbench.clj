(ns my-storage.getbench
  "get のレイテンシだけに絞った比較ベンチ。
   ウォームアップ + 多数回試行で、ノイズを排して block-size の効果を切り分ける。

   問い: 1M規模で get が 300us かかるのは
         (a) block-size のせいか  (b) 本物のディスクI/Oのせいか  (c) ノイズか"
  (:require [my-storage.core :as lsm])
  (:import [java.io File]))

(defn- rm-rf [^File f]
  (when (.isDirectory f) (doseq [c (.listFiles f)] (rm-rf c)))
  (.delete f))

(defn- pct [v p]
  (nth v (min (dec (count v)) (long (* p (count v))))))

(defn- heap-mb []
  (let [rt (Runtime/getRuntime)]
    (/ (- (.totalMemory rt) (.freeMemory rt)) 1048576.0)))

(defn- gc! [] (System/gc) (Thread/sleep 300))

(defn- index-entries [db]
  (reduce + 0 (map #(count (:index %)) (:sstables @(:state db)))))

(defn measure
  "n 件を書き込み、1枚にまとめてから get を測る。
   warmup 後に n-reads 回のランダム get を行い、分布を出す。"
  [label n val-size opts n-reads]
  (let [dir (str (doto (File/createTempFile "getbench" "") (.delete) (.mkdir)))
        db (lsm/open dir (merge {:flush-threshold (max 1000 (min 50000 (quot n 4)))
                                 :wal-fsync 1000} opts))
        v (apply str (repeat val-size \x))
        key-of (fn [i] (format "key%08d" i))]
    (try
      (dotimes [i n] (lsm/put db (key-of i) v))
      (lsm/compact! db)                     ; 1枚の大きな SSTable にする(条件を揃える)

      ;; ウォームアップ(JIT を温める。この分は計測しない)
      (let [rng (java.util.Random. 1)]
        (dotimes [_ 2000] (lsm/get db (key-of (.nextInt rng n)))))
      (gc!)

      (let [size (.length ^File (:file (first (:sstables @(:state db)))))
            idx (index-entries db)
            heap-before (heap-mb)
            rng (java.util.Random. 42)
            lat (vec (sort (for [_ (range n-reads)]
                             (let [i (.nextInt rng n)
                                   t (System/nanoTime)]
                               (lsm/get db (key-of i))
                               (- (System/nanoTime) t)))))
            st (lsm/stats db)]
        (println (format "%-26s file=%6.0fMB 索引=%,9d件 heap=%6.1fMB | p50=%7.1f p90=%7.1f p99=%8.1f us | read-amp=%.2f"
                         label (/ size 1048576.0) idx heap-before
                         (/ (pct lat 0.50) 1000.0)
                         (/ (pct lat 0.90) 1000.0)
                         (/ (pct lat 0.99) 1000.0)
                         (:read-amp st))))
      (finally
        (lsm/close db)
        (rm-rf (File. dir))))))

(defn -main [& args]
  (let [n (if (first args) (Long/parseLong (first args)) 1000000)
        vs (if (second args) (Long/parseLong (second args)) 1000)
        reads 3000]
    (println (format "\n=== get レイテンシ比較 (n=%,d / value=%dB / ウォームアップ2000回) ===" n vs))
    (println (format "1枚の大きな SSTable にまとめてから、ランダム get を %,d 回。\n" reads))

    (println "--- ① block-size を振る(同じ規模で比較)---")
    (measure "旧: index-interval=128" n vs {:index-interval 128} reads)
    (measure "新: block-size=4096"    n vs {:block-size 4096} reads)
    (measure "新: block-size=16384"   n vs {:block-size 16384} reads)
    (measure "新: block-size=65536"   n vs {:block-size 65536} reads)

    (println "\n--- ② 規模を変える(ディスクI/Oが支配的かの確認, block-size=4096固定)---")
    (println "   小さい規模ならページキャッシュに全部乗る → 速いはず")
    (doseq [scale [(quot n 20) (quot n 5) n]]
      (measure (format "n=%,d" scale) scale vs {:block-size 4096} reads))

    (println "\n読み方:")
    (println "  ①で block-size を変えても p50 が動かない → ブロックサイズは支配的でない")
    (println "  ②で規模を上げると p50 が跳ねる → 本物のディスクI/O(ページキャッシュに乗り切らない)")
    (shutdown-agents)))
