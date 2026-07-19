(ns my-storage.inverted
  (:require [my-storage.core :as core]
            [clojure.string :as str]))

(def ^:private SEP "|")

(defn- posting-key [term doc-id]
  (str "term" SEP term SEP doc-id))

(defn- term-prefix [term]
  (str "term" SEP term SEP))

(defn- successor [^String s]
  (str (subs s 0 (dec (count s)))
       (char (inc (int (.charAt s (dec (count s))))))))

(defn- tokenize [text]
  (->> (str/split (str/lower-case text) #"[^a-z0-9]+")
       (remove str/blank?)
       distinct))

(defn- intersect-sorted [xs ys]
  (loop [xs (seq xs)
         ys (seq ys)
         acc (transient [])]
    (if (and xs ys)
      (let [x (first xs)
            y (first ys)
            c (compare x y)]
        (cond
          (zero? c) (recur (next xs) (next ys) (conj! acc x))
          (neg? c) (recur (next xs) ys acc)
          :else (recur xs (next ys) acc)))
      (persistent! acc))))

(defn- union-sorted [xs ys]
  (loop [xs (seq xs)
         ys (seq ys)
         acc (transient [])]
    (cond
      (and xs ys)
      (let [x (first xs)
            y (first ys)
            c (compare x y)]
        (cond
          (zero? c) (recur (next xs) (next ys) (conj! acc x))
          (neg? c) (recur (next xs) ys (conj! acc x))
          :else (recur xs (next ys) (conj! acc y))))
      
      xs (persistent! (reduce conj! acc xs))
      ys (persistent! (reduce conj! acc ys))
      :else (persistent! acc))))

(defn index-document! [store doc-id text]
  (doseq [term (tokenize text)]
    (core/put store (posting-key term doc-id) ""))
  store)

(defn search-term [store term]
  (let [pre (term-prefix (str/lower-case term))]
    (->> (core/scan store pre (successor pre))
         (map (fn [[k _]] (subs k (inc (.lastIndexOf ^String k SEP)))))
         vec)))

(defn search-and [store terms]
  (let [lists (map #(search-term store %) terms)]
    (cond
      (empty? lists) []
      (some empty? lists) []
      :else (reduce intersect-sorted lists))))

(defn search-or [store terms]
  (let [lists (remove empty? (map #(search-term store %) terms))]
    (if (empty? lists)
      []
      (reduce union-sorted lists))))
