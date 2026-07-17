(ns my-storage.resp
  (:require [clojure.string :as str])
  (:import [java.io ByteArrayOutputStream DataInputStream]))

(defn- read-line-crlf
  "\r\n までを1行として読み、文字列で返す(末尾の \r\n は捨てる)。EOF なら nil。"
  [^DataInputStream in]
  (let [sb (StringBuilder.)]
    (loop []
      (let [b (.read in)]
        (cond
          (= b -1) (when (pos? (.length sb)) (.toString sb))
          (= b 13) (do (.read in) (.toString sb))
          :else (do (.append sb (char b)) (recur)))))))

(defn- read-bulk-string
  [^DataInputStream in ^long len]
  (let [buf (byte-array len)]
    (.readFully in buf)
    (.read in) (.read in)
    (String. buf "UTF-8")))

(defn read-command
  [^DataInputStream in]
  (when-let [header (read-line-crlf in)]
    (case (first header)
      \* (let [n (Integer/parseInt (subs header 1))]
           (vec (repeatedly
                 n
                 (fn []
                   (let [elem (read-line-crlf in)]
                     (if (= (first elem) \$)
                       (read-bulk-string in (Integer/parseInt (subs elem 1)))
                       elem))))))
      (vec (str/split header #" ")))))

(defn- crlf
  ^bytes [^String s]
  (.getBytes (str s "\r\n") "UTF-8"))

(defn encode
  ^bytes [v]
  (let [baos (ByteArrayOutputStream.)
        w (fn [^bytes b] (.write baos b 0 (alength b)))]
    (cond
      (= v :ok)
      (w (crlf "+OK"))

      (nil? v)
      (w (crlf "$-1"))

      (integer? v)
      (w (crlf (str ":" v)))

      (and (vector? v) (= (first v) :err))
      (w (crlf (str "-ERR " (second v))))
      
      :else (let [bs (.getBytes ^String v "UTF-8")]
              (w (crlf (str "$" (alength bs))))
              (w bs)
              (w (crlf ""))))
    (.toByteArray baos)))

