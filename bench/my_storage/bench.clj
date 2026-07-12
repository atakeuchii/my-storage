(ns my-storage.bench
  (:require [my-storage.core :as core])
  (:import [java.io File]))

(defn- temp-dir []
  (let [d (File/createTempFile "lsmbench" "")]
    (.delete d) (.mkdir d) (str d)))

(defn- rm-rf [^File f]
  (when (.isDirectory f)
    (doseq [c (.listFiles f)]
      (rm-rf c)))
  (.delete f))

(defn- percentile [sorted-vals p]
  (let [n (count sorted-vals)]
    (if (zero? n)
      0
      (nth sorted-vals (min (dec n) (long (* p n)))))))

(defn- key-of [i]
  (format "key%07d" i))

(defn bench-once [opts n n-reads]
  (let [dir (temp-dir)
        db (core/open dir opts)]
    (try
      (let [t0 (System/nanoTime)
            _ (dotimes [i n] (core/put db (key-of i) (str "v" i)))
            write-ns (- (System/nanoTime) t0)
            files (count (:sstables @(:state db)))
            bytes (->> (:sstables @(:state db))
                       (map #(.length ^File (:file %))) (reduce + 0))]
        (core/reset-stats! db)
        (let [rng (java.util.Random. 42)
              hit-lat (vec (sort (for [_ (range n-reads)]
                                   (let [i (.nextInt rng n)
                                         t (System/nanoTime)]
                                     (core/get db (key-of i))
                                     (- (System/nanoTime) t)))))
              hit-stats (core/stats db)]
          (core/reset-stats! db)
          (let [miss-lat (vec (sort (for [j (range n-reads)]
                                      (let [t (System/nanoTime)]
                                        (core/get db (str "absent" j))
                                        (- (System/nanoTime) t)))))
                miss-stats (core/stats db)]
            {:write-tps (long (/ n (/ write-ns 1e9)))
             :get-avg-us (/ (double (reduce + hit-lat)) (count hit-lat) 1000.0)
             :get-p99-us (/ (double (percentile hit-lat 0.99)) 1000.0)
             :miss-avs-us (/ (double (reduce + miss-lat)) (count miss-lat) 1000.0)
             :hit-read-amp (:read-amp hit-stats)
             :miss-read-amp (:read-amp miss-stats)
             :files files
             :bytes bytes})))
      (finally
        (core/close db)
        (rm-rf (File. dir))))))

(defn- fmt [label r]
  (format "%-28s tps=%-8d get=%6.1fus p99=%7.1fus miss=%6.1fus | read-amp hit=%4.2f miss=%4.2f | files=%-3d bytes=%d"
          label (:write-tps r) (:get-avg-us r) (:get-p99-us r) (:miss-avg-us r)
          (:hit-read-amp r) (:miss-read-amp r) (:files r) (:bytes r)))

(defn -main [& _]
  (let [n 20000
        n-reads 3000
        base {:flush-threshold 2000
              :compaction-threshold 4}]

    (println "=== WAL fsync ポリシー===")
    (doseq [p [:always 10 100 1000 :never]]
      (println (fmt (str "wal-fsync=" p)
                    (bench-once (assoc base :wal-fsync p) n n-reads))))

    (println "\n=== memtable 閾値(flush-threshold)===")
    (doseq [t [500 2000 8000]]
      (println (fmt (str "flush-threshold=" t)
                    (bench-once (assoc base :flush-threshold t :wal-fsync 100) n n-reads))))

    (println "\n=== Bloom 偽陽性率(bloom-fpp)===")
    (doseq [p [0.5 0.1 0.01 0.001]]
      (println (fmt (str "bloom-fpp=" p)
                    (bench-once (assoc base :bloom-fpp p :wal-fsync 100) n n-reads))))

    (println "\n=== スパースインデックス間隔(index-interval)===")
    (doseq [iv [16 128 1024]]
      (println (fmt (str "index-interval=" iv)
                    (bench-once (assoc base :index-interval iv :wal-fsync 100) n n-reads))))

    (println "\n=== コンパクション閾値(compaction-threshold)===")
    (doseq [c [2 4 8 nil]]
      (println (fmt (str "compaction-threshold=" c)
                    (bench-once (merge (assoc base :wal-fsync 100)
                                       (if c {:compaction-threshold c}
                                           {:compaction-threshold nil}))
                                n n-reads))))
    (shutdown-agents)))
