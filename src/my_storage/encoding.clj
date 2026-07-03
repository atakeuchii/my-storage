(ns my-storage.encoding
  "レコードのバイト直列化・復元。ファイルやストア状態には依存しない純粋な層。"
  (:import [java.io ByteArrayOutputStream DataOutputStream]
           [java.nio ByteBuffer]
           [java.util.zip CRC32]))

(def tombstone
  "削除マーカー。memtable / WAL / sstable で共有する唯一の tombstone センチネル。"
  ::tombstone)

(defn record-bytes
  "1レコードを [keyLen][key][valLen][value][crc] のバイト列にして返す。
   v が tombstone のときは valLen = -1 で value なし。"
  ^bytes [^String k v]
  (let [kb (.getBytes k "UTF-8")
        tomb? (= v tombstone)
        ^bytes vb (when-not tomb? (.getBytes ^String v "UTF-8"))
        baos (ByteArrayOutputStream.)
        dos (DataOutputStream. baos)]
    (.writeInt dos (alength kb))
    (.write dos kb)
    (if tomb?
      (.writeInt dos (int -1))
      (do (.writeInt dos (alength vb))
          (.write dos vb)))
    (.flush dos)
    (let [payload (.toByteArray baos)
          crc (doto (CRC32.) (.update payload))]
      (.writeInt dos (unchecked-int (.getValue crc)))
      (.flush dos)
      (.toByteArray baos))))

(defn try-read-record
  "buf の現在位置から1レコードを読む。壊れていれば nil を返す。"
  [^ByteBuffer buf]
  (let [start (.position buf)]
    (try
      (let [key-len (.getInt buf)
            _ (when (neg? key-len) (throw (ex-info "bad key-len" {})))
            kb (byte-array key-len)
            _ (.get buf kb)
            val-len (.getInt buf)
            tomb? (= val-len -1)
            vb (when-not tomb?
                 (when (neg? val-len) (throw (ex-info "bad val-len" {})))
                 (let [a (byte-array val-len)] (.get buf a) a))
            stored-crc (.getInt buf)
            end (.position buf)
            payload (byte-array (- end start 4))]
        (.position buf start)
        (.get buf payload)
        (.position buf end)
        (let [crc (doto (CRC32.) (.update payload))]
          (when (= (unchecked-int (.getValue crc)) stored-crc)
            {:k (String. kb "UTF-8")
             :v (if tomb? tombstone (String. vb "UTF-8"))
             :next-pos end})))
      (catch java.nio.BufferUnderflowException _
        nil)
      (catch clojure.lang.ExceptionInfo _
        nil))))
