(ns memento.redis.loader
  (:require [clojure.java.io :as io]
            [memento.base :as b]
            [memento.config :as mc]
            [memento.redis.keys :as keys]
            [memento.redis.listener :as listener]
            [memento.redis.sec-index :as sec-index]
            [taoensso.carmine :as car])
  (:import (memento.base EntryMeta InvalidationClock TagInvalidation)
           (memento.redis EntryEnvelope)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function BiConsumer)
           (java.util ArrayList List)
           (memento.redis.poll Load Loader LoaderSupport)))

(def ^ConcurrentHashMap maint
  "Map of conn to map of key to promise.

  For each connection the submap contains Redis keys to a Load object. In Load object:
  - If marker is present, then it's our load, and we must deliver to redis.
  - If not, a foreign JVM is going to deliver to Redis, and we must scan redis for it."
  (ConcurrentHashMap. 4 (float 0.75) 8))

(def load-marker-fade-sec
  "How long before a load marker fades. This is to prevent JVM exiting or dying from leaving LoadMarkers
  in Redis indefinitely, causing everyone to block on that key forever. This time is refreshed every
  second by a daemon thread, however a long GC will cause LoadMarkers to fade when they shouldn't.

  Adjust this setting appropriately via memento.redis.load_marker_fade system property."
  (Integer/parseInt (System/getProperty "memento.redis.load_marker_fade" "5")))

(def cached-entries-script (slurp (io/resource "memento/redis/poll/cached-entries.lua")))
(def refresh-load-markers-script (slurp (io/resource "memento/redis/poll/refresh-load-markers.lua")))

(defn latest-tag-invalidation
  "Compute the latest tag-invalidation epoch for a value about to be delivered."
  [v]
  (cond
    (instance? EntryMeta v)
    (.lastInvalidatedEpoch TagInvalidation/INSTANCE (.getTagIdents ^EntryMeta v))

    :else
    InvalidationClock/NO_INVALIDATION_EPOCH))

(defn cached-entries
  "Fetches keys to retrieve values stored by foreign JVMs.

  Returns a list of [key load-obj delivery-value] triples where delivery-value
  is one of:
    - :load-marker            → status 0: still a foreign load marker, keep waiting
    - b/absent                → status 2: key missing entirely (foreign loader died)
    - EntryMeta               → status 1: real value envelope, decoded

  See doc/future-tag-invalidation.md §5.9 for the cross-JVM joiner path semantics."
  [conn k-loads]
  (when (seq k-loads)
    (let [k-loads-vec (vec k-loads)
          raw-results (car/wcar conn
                        (car/parse-raw
                          (car/lua cached-entries-script (mapv first k-loads-vec) [])))]
      (keep (fn [[i status ^bytes env]]
              (let [[k load] (nth k-loads-vec (dec (long i)))]
                (case (long status)
                  0 nil
                  2 [k load b/absent]
                  1 (let [delivered (if (EntryEnvelope/isEntryEnvelope env)
                                       (EntryEnvelope/readEnvelope env)
                                       (do (car/wcar conn (car/del k)) b/absent))]
                      [k load delivered]))))
            raw-results))))

(defn refresh-load-markers
  "Refresh expire of load markers under keys ks, if they are still the same as we
  expect. markers must contain raw [0x03][16B UUID] byte arrays."
  [conn ^List ks ^List markers marker-fade]
  (when (seq ks)
      (let [argv (ArrayList.)]
        (doseq [m markers]
          (.add argv (car/raw m)))
      (.add argv marker-fade)
      (car/wcar conn
                (car/lua refresh-load-markers-script ks argv)))))

(defn maintain-conn-loads
  "Maintains the list of ongoing loads. It will fetch and fill promises for entries that were
  completed by other processes. If refresh-load-markers is true, it will also extends load markers
  this JVM owns.

  This should be only called from one thread."
  [conn ^ConcurrentHashMap loads-map refresh-load-markers?]
  (let [load-marker-keys (ArrayList.)
        load-markers (ArrayList.)
        upstream-keys (ArrayList.)
        extractor (reify BiConsumer
                     (accept [this k load]
                       (if-let [marker (.loadMarkerBytes ^Load load)]
                         (when refresh-load-markers?
                           (.add load-marker-keys k)
                           (.add load-markers marker))
                        (.add upstream-keys [k load]))))]
    (.forEach loads-map extractor)
    (when refresh-load-markers?
      (refresh-load-markers conn load-marker-keys load-markers load-marker-fade-sec))
    (doseq [[k load delivery] (cached-entries conn upstream-keys)]
      (when (.remove loads-map k load)
        (let [p (.getPromise ^Load load)]
          (.deliver p delivery (latest-tag-invalidation delivery))
          (.releaseResult p))))))

(defn maintenance-step
  "Perform maintenance multithreaded, one future per each connection after the first."
  [^ConcurrentHashMap maint refresh-load-markers?]
  ;; do maintenance of one conn in this thread, the rest in futures
  (let [[conn+map & more] (seq (.entrySet maint))
        futures (map #(future (maintain-conn-loads (key %) (val %) refresh-load-markers?)) more)]
    ;; process first conn in this thread and await others
    (when conn+map
      (do (maintain-conn-loads (key conn+map) (val conn+map) refresh-load-markers?)
          (doseq [f futures] (deref f))))))

(defn remove-load-markers
  "Remove all load markers owned by us from Redis. Useful when JVM is shutting down
  and we want any foreign JVMs on same cache to stop waiting for values that ain't happening."
  [^ConcurrentHashMap maint-map]
  (.forEach maint-map
            (reify BiConsumer
              (accept [this conn m]
                (car/wcar conn
                  (apply car/del
                         (reduce-kv (fn [l k ^Load v] (if (.getLoadMarker v) (conj l k) l)) '() m))))))
  (.clear maint-map))

;;;; END maintenance utilities

(def fetch-script (slurp (io/resource "memento/redis/fetch.lua")))

(defn fetch
  "Fetch a value, returns [present? val validation-epoch].

  Under the wire protocol (§5.1), `val` comes back as raw bytes (envelope
  bytes for cached values, 17-byte load-marker bytes for foreign / our load
  markers, nil for missing keys when load-ms is non-positive).

  load-marker is a UUID; it is serialized to its 17-byte wire
  shape via (car/raw (Load/loadMarkerBytes load-marker)) at the Lua call boundary
  so byte-equality inside abandon-load.lua / refresh-load-markers.lua /
  finish-load*.lua works.

  load-ms is load marker liveness in ms; if 0 or negative, no load marker is
  inserted (used by Loader.ifCached).

  fade-ms is fade setting for entries in ms if any."
  [conn k epoch-key load-marker load-ms fade-ms]
  (car/wcar conn
    (car/parse-raw
      (car/lua fetch-script
               {:k k :epoch-key epoch-key}
               {:fade-ms (if fade-ms fade-ms -1)
                :load (if (and load-ms (pos-int? load-ms)) 1 0)
                :load-marker (car/raw (Load/loadMarkerBytes load-marker))
                :load-ms load-ms}))))

(def abandon-load-script (slurp (io/resource "memento/redis/poll/abandon-load.lua")))
(def finish-load-script (slurp (io/resource "memento/redis/poll/finish-load.lua")))
(def put-value-script (slurp (io/resource "memento/redis/poll/put-value.lua")))
(def put-value-w-sec-script (slurp (io/resource "memento/redis/poll/put-value-w-sec.lua")))
(def bulk-set (slurp (io/resource "memento/redis/poll/bulk-set.lua")))

(def support
  (reify LoaderSupport
    (fetchEntry [this conn k load load-ms fade-ms]
      (fetch conn
             k
             (keys/epoch-key (.getKeysGenerator ^Load load) (.getCacheName ^Load load))
             (.getLoadMarker ^Load load)
             load-ms
             fade-ms))
    (fetchCachedEntry [this conn cname kg k fade-ms]
      (car/wcar conn
        (car/parse-raw
          (car/lua fetch-script
                   {:k k :epoch-key (keys/epoch-key kg cname)}
                   {:fade-ms (if fade-ms fade-ms -1)
                    :load 0
                    :load-marker (car/raw (byte-array 0))
                    :load-ms 0}))))
    (completeLoad [this conn key-list value load expire]
      ;; Serialize the loader result at the Redis boundary. completeLoad is only
      ;; used by claimed loader paths, so the marker is always a real UUID.
      (let [values {:load-marker (car/raw (.loadMarkerBytes ^Load load))
                    :v (car/raw (EntryEnvelope/writeEnvelope value))
                    :ttl-ms (or expire -1)
                    :validation-epoch (or (.getValidationEpoch ^Load load) -1)}]
        (if (= 1 (count key-list))
          (car/wcar conn (car/lua finish-load-script {:k (first key-list)} values))
          (let [ret (car/wcar conn (car/lua sec-index/finish-load-script key-list values))]
            (.put sec-index/all-indexes [conn (second key-list)] true)
            ret))))
    (putValue [this conn cname kg k value expire]
      ;; putValue is an explicit write, not a claimed load completion.
      (let [tag-idents (when (instance? EntryMeta value)
                         (let [entry ^EntryMeta value]
                           (when (.isNoCache entry)
                             (throw (IllegalArgumentException. "Cannot store a no-cache EntryMeta")))
                           (.getTagIdents entry)))
            values {:v (car/raw (EntryEnvelope/writeEnvelope value))
                    :ttl-ms (or expire -1)}]
        (if-let [tag-idents (seq tag-idents)]
          (let [key-list (sec-index/keys-param-for-sec-idx kg k cname tag-idents)
                ret (car/wcar conn (car/lua put-value-w-sec-script key-list values))]
            (.put sec-index/all-indexes [conn (second key-list)] true)
            ret)
          (car/wcar conn (car/lua put-value-script {:k k} values)))))
    (putValues [this conn keys values expire]
      (car/wcar conn
        (car/lua bulk-set
                  keys
                  (conj (mapv #(car/raw (EntryEnvelope/writeEnvelope %)) values)
                        (or expire -1)))))
    (abandonLoad [this conn key load]
      (car/wcar conn
        (car/lua abandon-load-script {:k key}
                 {:load-marker (car/raw (.loadMarkerBytes ^Load load))})))
    (completeLoadKeys [this cname kg k tag-idents]
      (if (seq tag-idents)
        (sec-index/keys-param-for-sec-idx kg k cname tag-idents)
        [k]))
    (ensureListener [this conn]
      (listener/ensure-l conn))
    (delEntry [this conn key]
      (car/wcar conn (car/del key)))))

(defn for-conf [conf keygen]
  (Loader. (:memento.redis/name conf "")
           keygen
           (mc/ret-fn conf)
           (mc/ret-ex-fn conf)
           (mc/ttl conf)
           (mc/fade conf)
           (:memento.redis/ttl-fn conf)
           (:memento.redis/hit-detect? conf false)
           maint
           support))
