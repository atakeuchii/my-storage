(ns my-storage.bloom
  (:import [java.util BitSet]
           [clojure.lang Murmur3]
           [java.io ByteArrayOutputStream DataOutputStream]
           [java.nio ByteBuffer]))

(defrecord Bloom [^BitSet bits ^long m ^int k])

(defn- optimal-m
  "期待要素数 n と目標偽陽性率 p から最適ビット数 m を計算する"
  ^long [^long n ^double p]
  (max 1 (long (Math/ceil (/ (* (- n) (Math/log p))
                             (* (Math/log 2) (Math/log 2)))))))

(defn- optimal-k
  "ビット数 m と要素数 n から最適ハッシュ本数 k を計算する"
  ^long [^long m ^long n]
  (max 1 (long (Math/round (* (/ (double m) (double (max 1 n))) (Math/log 2))))))

(defn create
  "n 件・偽陽性率 p 想定の空 Bloom を作る(既定 p=1%)。"
  ([n] (create n 0.01))
  ([n p]
   (let [m (optimal-m n p)
         k (optimal-k m n)]
     (->Bloom (BitSet. m) m (int k)))))

(defn- h1h2
  "文字列 key から独立な2つの 32bit ハッシュ h1,h2 を作る(long化)。"
  [^String key]
  (let [h1 (Murmur3/hashUnencodedChars key)
        h2 (Murmur3/hashLong h1)]
    [(long h1) (long h2)]))

(defn add!
  [^Bloom bf ^String key]
  (let [{:keys [^BitSet bits ^long m ^int k]} bf
        [h1 h2] (h1h2 key)]
    (dotimes [i k]
      (.set bits (int (Math/floorMod (+ h1 (* (long i) h2)) m))))
    bf))

(defn might-contain?
  [^Bloom bf ^String key]
  (let [{:keys [^BitSet bits ^long m ^int k]} bf
        [h1 h2] (h1h2 key)]
    (loop [i 0]
      (if (< i k)
        (if (.get bits (int (Math/floorMod (+ h1 (* (long i) h2)) m)))
          (recur (inc i))
          false)
        true))))

(defn to-bytes
  "Bloom をバイト列に変換する"
  ^bytes [^Bloom bf]
  (let [{:keys [^BitSet bits ^long m ^long k]} bf
        ba (.toByteArray bits)
        baos (ByteArrayOutputStream.)
        dos (DataOutputStream. baos)]
    (.writeLong dos m)
    (.writeInt dos (int k))
    (.writeInt dos (alength ba))
    (.write dos ba)
    (.flush dos)
    (.toByteArray baos)))

(defn from-buffer
  [^ByteBuffer buf]
  (let [m (.getLong buf)
        k (.getInt buf)
        len (.getInt buf)
        ba (byte-array len)]
    (.get buf ba)
    (->Bloom (BitSet/valueOf ba) m (int k))))
