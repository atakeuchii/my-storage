(ns my-storage.manifest
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import [java.io File FileOutputStream]
           [java.nio.file Files StandardCopyOption CopyOption]))

(def ^:private manifest-name "MANIFEST.edn")

(defn- manifest-file
  ^File [dir]
  (io/file dir manifest-name))

(defn load-manifest
  [dir]
  (let [f (manifest-file dir)]
    (if (.exists f)
      (edn/read-string (slurp f))
      {:sstables []})))

(defn save-manifest!
  [dir m]
  (let [f (manifest-file dir)
        tmp (io/file dir (str manifest-name ".tmp"))
        bytes (.getBytes (pr-str m) "UTF-8")]
    (with-open [out (FileOutputStream. tmp)]
      (.write out bytes)
      (.flush out)
      (.sync (.getFD out)))
    (Files/move (.toPath tmp) (.toPath f)
                (into-array CopyOption
                            [StandardCopyOption/ATOMIC_MOVE
                             StandardCopyOption/REPLACE_EXISTING]))))

(defn add-sstable!
  [dir filename]
  (save-manifest! dir (update (load-manifest dir) :sstables conj filename)))
