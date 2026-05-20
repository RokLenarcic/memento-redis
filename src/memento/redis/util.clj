(ns memento.redis.util
  (:require [memento.redis.keys :as keys]
            [taoensso.carmine :as car]))

(defn kv-by-pattern
  "Returns a map with all the keys matching the pattern and their values. The final
  map has keys transformed by key-xf-fn"
  ([conn key key-xf-fn]
   (kv-by-pattern conn key (constantly true) key-xf-fn))
  ([conn key key-pred key-xf-fn]
   (car/wcar conn
     (car/return
       (transduce
         (keep (fn [key-batch]
                 (let [matched-keys (filter key-pred key-batch)]
                   (when (seq matched-keys)
                     (interleave (map key-xf-fn matched-keys)
                                 (car/with-replies (apply car/mget matched-keys)))))))
         (completing (fn [acc kv-seq] (apply assoc acc kv-seq)))
         {}
         (keys/by-pattern key))))))

(defn del-keys-by-pattern
  [conn key]
  (car/wcar conn
    (doseq [key-batch (keys/by-pattern key)]
      (car/with-replies (apply car/del key-batch)))))

(defn nuke-keyspace
  "Nukes keyspace"
  [conn keygen]
  (doseq [k (keys/base-wildcard-keys keygen)]
    (del-keys-by-pattern conn k)))
