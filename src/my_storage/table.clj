(ns my-storage.table
  (:require [my-storage.core :as core]
            [clojure.edn :as edn]))

(def ^:private SEP "|")

(defn- enc-scalar [x]
  (if (integer? x)
    (format "%020d" x)
    (str x)))

(defn- pk-key [table pk]
  (str table SEP "r" SEP (enc-scalar pk)))

(defn- idx-key [table index v pk]
  (str table SEP "s" SEP index SEP (enc-scalar v) SEP (enc-scalar pk)))

(defn- idx-key-lo [table idx v]
  (str table SEP "s" SEP idx SEP (enc-scalar v) SEP))

(defn- idx-prefix [table index v]
  (str table SEP "s" SEP index SEP (enc-scalar v) SEP))

(defn- successor [^String s]
  (str (subs s 0 (dec (count s)))
       (char (inc (int (.charAt s (dec (count s))))))))

(defn- pk-part [^String k]
  (subs k (inc (.lastIndexOf k SEP))))

(defn insert-row! [store table pk-col indexes row]
  (let [pk (get row pk-col)]
    (core/put store (pk-key table pk) (pr-str row))
    (doseq [idx indexes]
      (core/put store (idx-key table idx (get row idx) pk) ""))
    store))

(defn find-by-pk
  [store table pk]
  (some-> (core/get store (pk-key table pk)) edn/read-string))

(defn find-by-index
  [store table idx value]
  (let [pre (idx-prefix table idx value)]
    (->> (core/scan store pre (successor pre))
         (map (fn [[k _]] (pk-part k)))
         (map #(find-by-pk store table (parse-long %)))
         (remove nil?)
         vec)))

(defn delete-row! [store table pk-col indexes row]
  (let [pk (get row pk-col)]
    (when-let [old (find-by-pk store table pk)]
      (doseq [idx indexes]
        (core/delete store (idx-key table idx (get old idx) pk)))
      (core/delete store (pk-key table pk)))
    store))

(defn update-row! [store table pk-col indexes row]
  (delete-row! store table pk-col indexes row)
  (insert-row! store table pk-col indexes row)
  store)

(defn range-by-index [store table idx lo hi]
  (let [from (idx-key-lo table idx lo)
        to (idx-key-lo table idx hi)]
    (->> (core/scan store from to)
         (map (fn [[k _]] (pk-part k)))
         (map #(find-by-pk store table (parse-long %)))
         (remove nil?)
         vec)))
 
(defn raw-keys
  "table に属する全キー(主キー行 + 索引)を辞書順で。構造を目で見る用。"
  [store table]
  (let [pre (str table SEP)]
    (map first (core/scan store pre (successor pre)))))
 