(ns my-storage.bloom-test
  (:require [clojure.test :refer [deftest is]]
            [my-storage.bloom :as bloom]))

(deftest no-false-negatives
  (let [bf (bloom/create 1000)
        ks (map #(str "key" %) (range 1000))]
    (doseq [k ks] 
      (bloom/add! bf k))
    (is (every? #(bloom/might-contain? bf %) ks))))

(deftest rejects-most-absent-keys
  (let [bf (bloom/create 1000 0.01)
        _ (doseq [i (range 1000)]
              (bloom/add! bf (str "present" i)))
        absent (map #(str "absent" %) (range 1000))
        fps (count (filter #(bloom/might-contain? bf %) absent))]
    (is (< fps 50) (str "false positives: " fps))))

(deftest roundtrip-serialization
  (let [bf (bloom/create 100)
        _ (doseq [i (range 100)]
            (bloom/add! bf (str "k" i)))
        buf (java.nio.ByteBuffer/wrap (bloom/to-bytes bf))
        bf2 (bloom/from-buffer buf)]
    (is (every? #(bloom/might-contain? bf2 (str "k" %)) (range 100)))))
