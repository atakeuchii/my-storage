(ns my-storage.property-test
  (:require [clojure.test :refer [use-fixtures]]
            ;; [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec] :as ctct]
            [my-storage.core :as core]))

(defn- temp-dir []
  (let [d (java.io.File/createTempFile "lsmprop" "")]
    (.delete d) (.mkdir d) (str d)))

(use-fixtures :once
  (fn [f]
    (binding [ctct/*default-test-count*
              (Integer/parseInt (or (System/getenv "TC_COUNT") "50"))]
      (f))))

;; 小さいキー空間 → 同じキーへの上書き・削除・再挿入が頻発する(バグを踏ませる)
(def ^:private key-space (mapv #(str "k" %) (range 8)))
(def ^:private gen-key (gen/elements key-space))
(def ^:private gen-val (gen/fmap str gen/nat))

(def ^:private gen-command
  (gen/frequency
   [[5 (gen/tuple (gen/return :put) gen-key gen-val)]
    [2 (gen/tuple (gen/return :delete) gen-key)]
    [1 (gen/return [:compact])]]))

(def ^:private gen-commands (gen/vector gen-command 0 50))

(defn- model-apply [model [op a b]]
  (case op
    :put (assoc model a b)
    :delete (dissoc model a)
    :compact model))

(defn- engine-apply! [db [op a b]]
  (case op
    :put (core/put db a b)
    :delete (core/delete db a)
    :compact (core/compact! db))
  db)

(defn- states-match? [db model]
  (and (every? (fn [k] (= (core/get db k) (get model k))) key-space)
       (= (mapv (fn [[k v]] [k v]) (core/scan db nil nil))
          (mapv (fn [e] [(key e) (val e)]) model))))

(defn- run-commands [commands opts]
  (let [db (core/open (temp-dir) opts)]
    (try
      (loop [cmds commands
             model (sorted-map)]
        (if-let [cmd (first cmds)]
          (let [model' (model-apply model cmd)]
            (engine-apply! db cmd)
            (if (states-match? db model')
              (recur (rest cmds) model')
              false))
          true))
      (finally (core/close db)))))

(defspec engine-equivalent-to-sorted-map
  (prop/for-all [commands gen-commands]
                (run-commands commands {:flush-threshold 3 :compaction-threshold 3})))

(defspec scan-range-equivalent-to-sorted-map
  (prop/for-all [commands gen-commands
                 start gen-key
                 end gen-key]
                (let [db (core/open (temp-dir) {:flush-threshold 3 :compaction-threshold 3})]
                  (try
                    (let [model (reduce model-apply (sorted-map) commands)]
                      (doseq [c commands] (engine-apply! db c))
                      (let [[lo hi] (sort [start end])]
                        (= (mapv (fn [[k v]] [k v]) (core/scan db lo hi))
                           (mapv (fn [e] [(key e) (val e)]) (subseq model >= lo < hi)))))
                    (finally (core/close db))))))

(def ^:private gen-command-r
  (gen/frequency
   [[5 (gen/tuple (gen/return :put) gen-key gen-val)]
    [2 (gen/tuple (gen/return :delete) gen-key)]
    [1 (gen/return [:compact])]
    [1 (gen/return [:restart])]]))

(def ^:private gen-commands-r (gen/vector gen-command-r 0 50))

(defn- run-with-restart [commands opts]
  (let [dir (temp-dir)]
    (loop [cmds commands
           model (sorted-map)
           db (core/open dir opts)]
      (if-let [[op a b] (first cmds)]
        (if (= op :restart)
          (do (core/close db)
              (let [db2 (core/open dir opts)]
                (if (states-match? db2 model)
                  (recur (rest cmds) model db2)
                  (do (core/close db2) false))))
          (let [model' (model-apply model [op a b])]
            (engine-apply! db [op a b])
            (if (states-match? db model')
              (recur (rest cmds) model' db)
              (do (core/close db) false))))
        (do (core/close db) true)))))

(defspec engine-equivalent-across-restart
  (prop/for-all [commands gen-commands-r]
                (run-with-restart commands {:flush-threshold 3 :compaction-threshold 3})))
