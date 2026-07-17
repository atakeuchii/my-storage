(ns my-storage.server
  (:require [clojure.string :as str]
            [my-storage.core :as core]
            [my-storage.resp :as resp])
  (:import [java.net ServerSocket Socket]
           [java.io DataInputStream BufferedInputStream OutputStream]
           [clojure.lang ExceptionInfo]))

(defn dispatch
  [store [cmd & args]]
  (case (str/upper-case cmd)
    "PING" (if (seq args) (first args) "PONG")
    "SET"  (let [[k v] args] (core/put store k v) :ok)
    "GET"  (core/get store (first args))
    "DEL"  (let [k (first args)
                 existed (some? (core/get store k))]
             (core/delete store k)
             (if existed 1 0))
    "COMMAND" :ok
    [:err (str "unknown command '" cmd "'")]))

(defn handle-client
  [store ^Socket sock]
  (with-open [sock sock
              in (DataInputStream. (BufferedInputStream. (.getInputStream sock)))]
    (let [^OutputStream out (.getOutputStream sock)]
      (loop []
        (when-let [cmd (resp/read-command in)]
          (let [resp-bytes (try
                             (resp/encode (dispatch store cmd))
                             (catch ExceptionInfo e
                               (resp/encode [:err (.getMessage e)])))]
            (.write out ^bytes resp-bytes)
            (.flush out)
            (recur)))))))

(defn start
  [store port]
  (let [server (ServerSocket. port)]
    (println "[server] listening on" port)
    (loop []
      (let [sock (.accept server)]
        (future (handle-client store sock)))
      (recur))))
