(ns my-storage.core-test
  (:require [clojure.test :refer [deftest is]]
            [my-storage.core :as core]))

(deftest put-then-get
  (let [db (core/open)]
    (core/put db "k" "v")
    (is (= "v" (core/get db "k")))))

(deftest overwrite
  (let [db (core/open)]
    (core/put db "k" "v1")
    (core/put db "k" "v2")
    (is (= "v2" (core/get db "k")))))

(deftest missing-key
  (let [db (core/open)]
    (is (nil? (core/get db "nope")))))

(deftest delete-removes-key
  (let [db (core/open)]
    (core/put db "k" "v")
    (core/delete db "k")
    (is (nil? (core/get db "k")))))

(deftest keys-are-sorted
  (let [db (core/open)]
    (doseq [k ["c" "a" "b" "delta" "alpha"]]
      (core/put db k k))
    (is (= ["a" "alpha" "b" "c" "delta"]
           (keys @(:data db))))))
