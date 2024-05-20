(ns memento.redis.keys
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.protocol :as p])
  (:import (java.nio ByteBuffer)
           (java.security MessageDigest)
           (memento.base Segment)))

(def ^MessageDigest sha1-digest (MessageDigest/getInstance "sha1"))
(def ^MessageDigest sha256-digest (MessageDigest/getInstance "sha-256"))

(defn digest [dig-name o]
  (.digest
    (case dig-name
      :sha-256 sha256-digest
      :sha1 sha1-digest)
    (p/byte-str o)))

(defn wildcard
  "Creates a wildcard key from a list key. It will select all keys with additional
  elements in the list. It will not select the key itself.

  Need to use REAL list, not list*. Metadata on the list is ignored as it screws up wildcarding

  Escapes bytes ? * [ \\ from provided key."
  [key]
  (let [^bytes b-arr (p/byte-str (with-meta key nil))
        b-arr-len (alength b-arr)
        ;; set the list count to ?
        _ (aset b-arr 7 (byte 63))
        ^ByteBuffer bb (.put (ByteBuffer/allocate (* 2 (alength b-arr)))
                             b-arr 0 8)]
    (loop [i 8]
      (when (< i b-arr-len)
        (let [b (aget b-arr i)]
          (case b
            ;; escape these bytes
            (42 63 92 91) (.put bb (byte 92))
            nil)
          (.put bb b)
          (recur (inc i)))))
    ;; add last * and prepare for reading
    (let [output (byte-array (-> bb (.put (byte 42)) .flip .remaining))
          _ (.get bb output)]
      (p/raw output))))

(defn by-pattern
  "Returns a SCAN result sequence, partitioned to 1000 items per batch.
   Needs to be called inside car/wcar.

   Returns an eduction."
  [key]
  (let [scan-from-cursor #(car/with-replies (car/scan % "COUNT" 100 "MATCH" key))]
    (eduction
      (take-while some?)
      (mapcat #(% 1))
      (partition-all 1000)
      (iterate
        (fn [[cursor entries]]
          (when (and cursor (not= cursor "0"))
            (scan-from-cursor cursor)))
        (scan-from-cursor "0")))))

(defprotocol KeysGenerator
  "Generates keys to be used when working with Redis."
  (cache-wildcard-key [this cache-name]
    "A wildcard (glob) key that will return all keys used by a particular cache.")
  (cache-key? [this cache-name k]
    "Returns true if key belongs to the named cache")
  (segment-wildcard-key [this cache-name segment]
    "A wildcard (glob) key that will return all keys used by a particular memoized
    function. Segment is memento.base.Segment.")
  (segment-key? [this cache-name segment k]
    "Returns true if key belongs to the Segment")
  (entry-key [this cache-name segment args-key]
    "A concrete key for an entry. Segment is memento.base.Segment.")
  (sec-index-id-key [this cache-name id]
    "ID for the SET that houses all the keys that get invalidated by id.")
  (sec-indexes-key [this]
    "ID for the SET that houses all base keys of secondary indexes.")
  (base-wildcard-keys [this]
    "Returns a coll of wildcard keys that will select all keys of all Memento Redis caches
    that use this kind of Keys Generator, used for nuking all the caches."))

(defn default-keys-generator [memento-space-prefix
                              memento-secondary-index-prefix
                              anon-key-strat]
  (let [segment-id-fn (case anon-key-strat
                        :stringify #(if (fn? %) (str %) %)
                        :empty #(if (fn? %) nil %))]
    (reify KeysGenerator
      (cache-wildcard-key [this cache-name]
        (wildcard (list memento-space-prefix cache-name)))
      (cache-key? [this cache-name k]
        (let [[prefix n] k]
          (and (= prefix memento-space-prefix) (= n cache-name))))
      (segment-wildcard-key [this cache-name segment]
        (wildcard (list memento-space-prefix cache-name (segment-id-fn (.getId ^Segment segment)))))
      (segment-key? [this cache-name segment k]
        (let [[prefix n segment-id] k]
          (and (= prefix memento-space-prefix) (= n cache-name) (= segment-id (segment-id-fn (.getId ^Segment segment))))))
      (entry-key [this cache-name segment args-key]
        (list memento-space-prefix cache-name (segment-id-fn (.getId ^Segment segment)) args-key))
      (sec-index-id-key [this cache-name id]
        (list memento-secondary-index-prefix cache-name id))
      (sec-indexes-key [this]
        (list memento-secondary-index-prefix nil))
      (base-wildcard-keys [this]
        [(wildcard (list memento-space-prefix))
         (wildcard (list memento-secondary-index-prefix))]))))
