(ns my-storage.merge-test
  (:require [clojure.test :refer [deftest is]]
            [my-storage.encoding :as enc]
            [my-storage.merge :as merge]))

(deftest merges-in-sorted-order
  (is (= [["a" "1"] ["b" "2"] ["c" "3"] ["d" "4"]]
         (merge/merge-sorted [[["a" "1"] ["c" "3"]]
                              [["b" "2"] ["d" "4"]]])))) 

(deftest newst-source-wins-on-tie
  (is (= [["k" "new"] ["x" "1"] ["y" "2"]]
         (merge/merge-sorted [[["k" "new"]]
                              [["k" "old"] ["x" "1"]]
                              [["y" "2"]]]))))

(deftest keeps-tombstones
  (is (= [["a" "1"] ["b" enc/tombstone] ["c" "3"]]
         (merge/merge-sorted [[["b" enc/tombstone]]
                              [["a" "1"] ["b" "old"] ["c" "3"]]]))))

(deftest handles-empty-sources
  (is (= [["a" "1"] ["b" "2"]]
         (merge/merge-sorted [[] [["a" "1"]] [] [["b" "2"]]]))))

(deftest merges-in-desc-order
  (is (= [["d" "4"] ["c" "3"] ["b" "2"] ["a" "1"]]
         (merge/merge-sorted-desc [[["c" "3"] ["a" "1"]]
                                   [["d" "4"] ["b" "2"]]]))))

(deftest desc-newest-source-wins-on-tie
  (is (= [["y" "2"] ["x" "1"] ["k" "new"]]
         (merge/merge-sorted-desc [[["k" "new"]]
                                   [["x" "1"] ["k" "old"]]
                                   [["y" "2"]]]))))
