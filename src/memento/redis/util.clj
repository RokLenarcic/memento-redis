(ns memento.redis.util
  (:require [memento.redis.keys :as keys]
            [taoensso.carmine :as car])
  (:import (memento.base EntryMeta)))

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

(defn kv-by-pattern-raw
  "Like kv-by-pattern but reads values with car/parse-raw so each value is a
  byte[] (or nil for missing keys). Applies val-xf-fn to each raw byte[]; pairs
  for which val-xf-fn returns EntryMeta/absent are elided from the result.

  Used by cache.clj/asMap to expose decoded entry values where val-xf-fn
  decodes Redis entry envelopes. Nil is a legitimate cached value; EntryMeta/absent
  is the explicit sentinel for in-flight load markers (0x03) and ignored values."
  [conn key key-pred key-xf-fn val-xf-fn]
  (car/wcar conn
    (car/return
      (transduce
        (keep (fn [key-batch]
                (let [matched-keys (filter key-pred key-batch)]
                  (when (seq matched-keys)
                    (let [raw-vals (car/with-replies
                                     (car/parse-raw (apply car/mget matched-keys)))
                          pairs (->> (map vector matched-keys raw-vals)
                                      (keep (fn [[k v]]
                                              (let [xv (val-xf-fn v)]
                                                (when-not (identical? EntryMeta/absent xv)
                                                  [(key-xf-fn k) xv])))))]
                      (when (seq pairs)
                        (apply concat pairs)))))))
        (completing (fn [acc kv-seq] (apply assoc acc kv-seq)))
        {}
        (keys/by-pattern key)))))

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
