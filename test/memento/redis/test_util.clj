(ns memento.redis.test-util
  (:require [memento.base :as b]
            [taoensso.carmine :as car]
            [memento.redis.cache]
            [memento.redis.poll.daemon :as daemon]
            [memento.redis.keys :as keys]
            [memento.redis.loader :as loader]
            [memento.redis.util :as util]
            [memento.core :as core])
  (:import (memento.base EntryMeta Segment)
            (memento.redis EntryEnvelope)
            (memento.redis.cache RedisCache)
            (memento.redis.poll Load)
            (java.util UUID)))

(defn sprn [& args]
  (locking println
    (apply println args)))

(do @daemon/daemon-thread)

(def prefix "MMR-TEST")

(def test-keygen
  (keys/default-keys-generator
    prefix
    "MMRS-TEST"
    :stringify))

(defn test-key
  "An entry key in test keyspace."
  [k]
  (keys/entry-key test-keygen "" (Segment. identity identity "" {}) k))

(defn- ->wire-bytes
  "Coerce a test fixture value to the bytes that should land in Redis under the
  post-spec wire format (§5.1):
    UUID           → 17-byte [0x03][16B UUID] form
    byte[]         → passed through (caller has already prepared envelope bytes)
    anything else  → envelope-frozen via EntryEnvelope/writeEnvelope (auto-discriminator
                     selection by tag presence)"
  ^bytes [v]
  (cond
    (instance? UUID v) (Load/loadMarkerBytes v)
    (bytes? v) v
    :else (EntryEnvelope/writeEnvelope v)))

(defn add-entry
  "Add a full test generator keyed entry. Encodes v to the §5.1 wire format
  before writing so reads via car/parse-raw observe a real envelope."
  [k v]
  (car/wcar {} (car/set (test-key k) (car/raw (->wire-bytes v)))))

(defn get-entry
  "Retrieve a full test generator keyed entry and decode it back to a
  test-friendly Clojure value:
    nil bytes              → nil
    17-byte 0x03 marker    → UUID
    0x01/0x02 envelope     → user value (EntryMeta.getV)
    anything else (legacy) → returned as-is (raw bytes)"
  [k]
  (let [^bytes raw (car/wcar {} (car/parse-raw (car/get (test-key k))))]
    (cond
      (nil? raw) nil
      (Load/isLoadMarker raw) (Load/loadMarker raw)
      (and (pos? (alength raw))
            (or (= EntryEnvelope/TAG_UNTAGGED (aget raw 0))
                (= EntryEnvelope/TAG_TAGGED   (aget raw 0))))
          (EntryMeta/unwrap (EntryEnvelope/readEnvelope raw))
      :else raw)))

(defn get-entry*
  "Retrieve the arg list value from Redis (decoded via get-entry path)."
  [f args]
  (let [cache (core/active-cache f)
        segment (.segment ^Segment f)
        k ((-> cache :fns :key-fn) segment args)]
    (when (instance? RedisCache cache)
      (let [^bytes raw (car/wcar ((-> cache :fns :conn)) (car/parse-raw (car/get k)))]
        (cond
          (nil? raw) nil
          (Load/isLoadMarker raw) (Load/loadMarker raw)
          (and (pos? (alength raw))
                (or (= EntryEnvelope/TAG_UNTAGGED (aget raw 0))
                    (= EntryEnvelope/TAG_TAGGED   (aget raw 0))))
      (EntryMeta/unwrap (EntryEnvelope/readEnvelope raw))
          :else raw)))))

(defmacro with-kv
  "Insert and delete the specified key values, prefixing the string. This is raw keys values"
  [prefix m & body]
  `(try
     (car/wcar {}
       (apply car/mset
              (interleave
                (map (partial str ~prefix) (keys ~m))
                (vals ~m))))
     ~@body
     (finally
       (let [ks# (car/wcar {} (car/keys (str ~prefix "*")))]
         (when-not (empty? ks#)
           (car/wcar {} (apply car/del ks#)))))))

(defn wipe []
  (util/nuke-keyspace {} test-keygen)
  (.clear loader/maint))

(defn unwrap-as-map
  "Helper for tests that previously expected `as-map` to return plain values.
  Post-§5.4.3 `as-map` returns `Map<key, EntryMeta>`; this helper unwraps to
  `Map<key, value>` so existing equality assertions keep working."
  [m]
  (into {} (map (fn [[k v]]
                  [k (if (instance? EntryMeta v) (.getV ^EntryMeta v) v)]))
        m))

(defn fixture-wipe [f]
  (wipe)
  (try
    (f)
    (finally (wipe))))
