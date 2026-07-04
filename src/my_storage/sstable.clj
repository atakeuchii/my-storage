(ns my-storage.sstable
  "sorted-map を SSTable ファイルとして書き出す層。"
  (:require [clojure.java.io :as io]
            [my-storage.encoding :as enc])
  (:import [java.io FileOutputStream DataOutputStream BufferedOutputStream File]
           [java.nio ByteBuffer]
           [java.nio.channels FileChannel]
           [java.nio.file StandardOpenOption OpenOption]
           [java.util.concurrent.atomic AtomicLong]))

(def ^:private sstable-magic
  (.getBytes "MYSSTBL1" "UTF-8"))

(def ^:private index-interval
  128)

(def ^:private footer-size 20)

(defonce ^:private sstable-counter (AtomicLong. 0))

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
  (io/file dir (format "sstable-%013d-%09d.db"
                       (System/currentTimeMillis)
                       (.getAndIncrement sstable-counter))))

(defn list-sstable-files
  [^File dir]
  (->> (.listFiles dir)
       (filter #(.endsWith (.getName ^File %) ".db"))
       (sort-by #(.getName ^File %))
       vec))

(defrecord SSTableReader [^FileChannel ch ^File file index ^long index-offset ^long entry-count])

(defn- read-fully
  [^FileChannel ch ^ByteBuffer buf ^long pos]
  (loop [p pos]
    (when (.hasRemaining buf)
      (let [n (.read ch buf p)]
        (when (pos? n)
          (recur (+ p n)))))))

(defn open-reader
  [^File file]
  (let [ch (FileChannel/open (.toPath file)
                             (into-array OpenOption [StandardOpenOption/READ]))
        size (.size ch)
        fbuf (ByteBuffer/allocate footer-size)]
    (read-fully ch fbuf (- size footer-size))
    (.flip fbuf)
    (let [index-offset (.getLong fbuf)
          entry-count (.getInt fbuf)
          magic (let [m (byte-array 8)] (.get fbuf m) (String. m "UTF-8"))]
      (when-not (= magic "MYSSTBL1")
        (.close ch)
        (throw (ex-info "bad sstable magic" {:file (str file) :magic magic})))
      (let [ibuf (ByteBuffer/allocate (int (- (- size footer-size) index-offset)))]
        (read-fully ch ibuf index-offset)
        (.flip ibuf)
        (let [idx-count (.getInt ibuf)
              index (loop [i 0
                           acc (transient [])]
                      (if (< i idx-count)
                        (let [kl (.getInt ibuf)
                              kb (byte-array kl)
                              _ (.get ibuf kb)
                              off (.getLong ibuf)]
                          (recur (inc i) (conj! acc [(String. kb "UTF-8") off])))
                        (persistent! acc)))]
          (->SSTableReader ch file index index-offset entry-count))))))

(defn close-reader!
  [^SSTableReader reader]
  (.close ^FileChannel (:ch reader)))

(defn- read-entry
  [^ByteBuffer buf]
  (let [key-len (.getInt buf)
        kb (byte-array key-len)
        _ (.get buf kb)
        val-len (.getInt buf)
        k (String. kb "UTF-8")]
    (if (= val-len -1)
      [k enc/tombstone]
      (let [vb (byte-array val-len)]
        (.get buf vb)
        [k (String. vb "UTF-8")]))))

(defn- find-block
  [index ^long index-offset target]
  (let [n (count index)]
    (loop [lo 0
           hi (dec n)
           best -1]
      (if (<= lo hi)
        (let [mid (quot (+ lo hi) 2)
              k (first (nth index mid))]
          (if (<= (compare k target) 0)
            (recur (inc mid) hi mid)
            (recur lo (dec mid) best)))
        (when (>= best 0)
          [(long (second (nth index best)))
           (long (if (< (inc best) n)
                   (second (nth index (inc best)))
                   index-offset))])))))

(defn sstable-get
  "reader から target を引く。値(文字列) OR enc/tombstone OR enc/not-found を返す。"
  [^SSTableReader reader target]
  (let [{:keys [^FileChannel ch index index-offset]} reader]
    (if-let [[start end] (find-block index index-offset target)]
      (let [buf (ByteBuffer/allocate (int (- end start)))]
        (read-fully ch buf start)
        (.flip buf)
        (loop []
          (if (.hasRemaining buf)
            (let [[k v] (read-entry buf)
                  c (compare k target)]
              (cond
                (zero? c) v
                (pos? c) enc/not-found
                :else (recur)))
            enc/not-found)))
      enc/not-found)))
