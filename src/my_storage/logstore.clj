(ns my-storage.logstore
  "my-storage を『時系列ログストア』として使うための薄いラッパー。
   キー = event:<13桁ゼロ埋めms>:<連番> で、辞書順 = 時刻順を保証する。"
  (:require [my-storage.core :as core]
            [clojure.edn :as edn])
  (:import [java.time Instant ZoneId]
           [java.time.format DateTimeFormatter]))

(def ^:private seq-counter (atom 0))

(defn- next-seq [] (format "%06d" (swap! seq-counter inc)))

(defn event-key [^long ts]
  (format "event:%013d:%s" ts (next-seq)))

(defn ts-bound [^long ts]
  (format "event:%013d:" ts))

(defn log!
  "1件ログを書く。ts 省略時は現在時刻。value は EDN で保存。"
  ([store level msg] (log! store (System/currentTimeMillis) level msg))
  ([store ^long ts level msg]
   (core/put store (event-key ts) (pr-str {:ts ts :level level :msg msg}))
   store))

(defn- decode [v] (when v (edn/read-string v)))

(defn events-between
  "[from-ts, to-ts) のログを時刻順に返す。"
  [store from-ts to-ts]
  (map (comp decode second)
       (core/scan store (ts-bound from-ts) (ts-bound to-ts))))

(defn events-since
  "現在時刻から遡って ms ミリ秒ぶんのログ。"
  [store ms]
  (let [now (System/currentTimeMillis)]
    (events-between store (- now ms) now)))

;; ---- 見やすく出す ----

(def ^:private fmt
  (.withZone (DateTimeFormatter/ofPattern "MM/dd HH:mm:ss")
             (ZoneId/of "Asia/Tokyo")))

(defn print-events [events]
  (doseq [{:keys [ts level msg]} events]
    (println (format "%s  [%-5s]  %s"
                     (.format fmt (Instant/ofEpochMilli ts))
                     (name level) msg)))
  (println (format "-- %d 件 --" (count events))))

(defn all-events
  "全イベントを時刻順(古い→新しい)に返す。確認・デバッグ用。"
  [store]
  (map (comp decode second)
       (core/scan store "event:" "event;")))   ; ';' は ':' の次の文字

(defn oldest-events
  "古いものから n 件。辞書昇順の先頭 n 件そのもの。"
  [store n]
  (->> (core/scan store "event:" "event;")
       (take n)
       (map (comp decode second))))

(defn latest-events
  [store n]
  (->> (core/rscan store "event:" "event;")
       (take n)
       (map (comp decode second))))

(defn all-keys
  "格納されている全キーを辞書順(=時刻順)で返す。確認用。"
  [store]
  (map first (core/scan store "event:" "event;")))

(defn seed-logs! [store n spread-ms]
  (let [now (System/currentTimeMillis)
        levels [:info :info :info :warn :error]]  ; info多め
    (dotimes [i n]
      (let [ts  (- now (long (rand-int spread-ms)))
            lvl (rand-nth levels)]
        (log! store ts lvl (str "event #" i " (" (name lvl) ")"))))
    (println (format "seeded %d logs over past %.1f h" n (/ spread-ms 3600000.0)))))
