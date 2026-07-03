(ns my-storage.sstable
  "sorted-map を SSTable ファイルとして書き出す層。"
  (:require [clojure.java.io :as io]
            [my-storage.encoding :as enc])
  (:import [java.io FileOutputStream DataOutputStream BufferedOutputStream File]))

(def ^:private sstable-magic
  (.getBytes "MYSSTBL1" "UTF-8"))

(def ^:private index-interval
  128)

(defn- write-entry!
  [^DataOutputStream dos ^String k v]
  (let [kb (.getBytes k "UTF-8")]
    (.writeInt dos (alength kb))
    (.write dos kb)
    (if (= v enc/tombstone)
      (.writeInt dos (int -1))
      (let [vb (.getBytes ^String v "UTF-8")]
        (.writeInt dos (alength vb))
        (.write dos vb)))))

(defn write-sstable!
  "entries(sorted) を sstable として書き出す。index-interval ごとに index を作る。"
  [^File file entries]
  (with-open [fos (FileOutputStream. file)
              dos (DataOutputStream. (BufferedOutputStream. fos))]
    (let [index (loop [es (seq entries)
                       i 0
                       acc (transient [])]
                  (if-let [[k v] (first es)]
                    (let [offset (.size dos)]
                      (write-entry! dos k v)
                      (recur (next es)
                             (inc i)
                             (if (zero? (mod i index-interval))
                               (conj! acc [k offset])
                               acc)))
                    (persistent! acc)))
          entry-count (count entries)
          index-offset (.size dos)]
      (.writeInt dos (count index))
      (doseq [[^String k ^long offset] index]
        (let [kb (.getBytes k "UTF-8")]
          (.writeInt dos (alength kb))
          (.write dos kb)
          (.writeLong dos offset)))
      (.writeLong dos index-offset)
      (.writeInt dos entry-count)
      (.write dos sstable-magic)
      (.flush dos)
      {:file file :entry-count entry-count :index-entries (count index)})))

(defn next-sstable-file
  [^File dir]
  (io/file dir (format "sstable-%013d.db" (System/currentTimeMillis))))
