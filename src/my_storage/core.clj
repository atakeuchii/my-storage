(ns my-storage.core
  (:refer-clojure :exclude [get]))

(defprotocol KVStore
  (-put [this k v])
  (-get [this k])
  (-delete [this k])
  (-close [this]))

(defrecord MemoryStore [data]
  KVStore
  (-put [this k v]
    (swap! data assoc k v)
    this)
  (-get [this k]
    (clojure.core/get @data k))
  (-delete [this k]
    (swap! data dissoc k)
    this)
  (-close [this] nil))

(defn put [store k v] (-put store k v))
(defn get [store k] (-get store k))
(defn delete [store k] (-delete store k))
(defn close [store] (-close store))

(defn open 
  []
  (->MemoryStore (atom (sorted-map))))
