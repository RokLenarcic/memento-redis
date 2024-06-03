(ns memento.redis.loader
  (:require [clojure.java.io :as io]
            [memento.base :as b]
            [memento.config :as mc]
            [memento.redis.listener :as listener]
            [memento.redis.sec-index :as sec-index]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.tools :as nippy-tools]
            [taoensso.carmine :as car])
  (:import (java.io DataInput DataOutput)
           (memento.base EntryMeta)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function BiFunction BiConsumer)
           (clojure.lang Keyword)
           (java.util ArrayList List UUID)
           (memento.redis.poll Load Loader LoaderSupport)))

(def ^ConcurrentHashMap maint
  "Map of conn to map of key to promise.

  For each connection the submap contains Redis keys to a Load object. In Load object:
  - If marker is present, then it's our load, and we must deliver to redis.
  - If not, a foreign JVM is going to deliver to Redis, and we must scan redis for it."
  (ConcurrentHashMap. 4 (float 0.75) 8))

(nippy/extend-freeze EntryMeta ::b/entry-meta [^EntryMeta x data-output]
                     (nippy/-freeze-with-meta! (.getV x) data-output)
                     (.writeBoolean ^DataOutput data-output (.isNoCache x))
                     (nippy/-freeze-with-meta! (.getTagIdents x) data-output))

(nippy/extend-thaw ::b/entry-meta [data-input]
  (EntryMeta. (nippy/thaw-from-in! data-input) (.readBoolean ^DataInput data-input) (nippy/thaw-from-in! data-input)))

(defrecord LoadMarker [x])

(defn new-load-marker [] (->LoadMarker (UUID/randomUUID)))

(def load-marker-fade-sec
  "How long before a load marker fades. This is to prevent JVM exiting or dying from leaving LoadMarkers
  in Redis indefinitely, causing everyone to block on that key forever. This time is refreshed every
  second by a daemon thread, however a long GC will cause LoadMarkers to fade when they shouldn't.

  Adjust this setting appropriately via memento.redis.load_marker_fade system property."
  (Integer/parseInt (System/getProperty "memento.redis.load_marker_fade" "5")))

(defn cval
  "Carmine saves Numbers and Keywords as Strings, so when you fetch them, they are just left as strings,
  so we force Nippy serialization. We also force Strings to nippy so we can get the benefit of automatic
  LZ4 compression on big strings (like people dumping whole JSONs and HTML responses into our DB)."
  [v]
  (condp instance? v
    String (nippy-tools/wrap-for-freezing v)
    Number (nippy-tools/wrap-for-freezing v)
    Keyword (nippy-tools/wrap-for-freezing v)
    v))

(def cached-entries-script (slurp (io/resource "memento/redis/poll/cached-entries.lua")))
(def refresh-load-markers-script (slurp (io/resource "memento/redis/poll/refresh-load-markers.lua")))

(defn cached-entries
  "Fetches keys, to retrieve values entered/calculated by a foreign cache (other JVM).

  If load marker is still there, the key is not returned.
  If a value is there, we return a [key value] pair.
  If the value is not there, then the marker has expired, the foreign loader is dead or stalled.
  In that case the script returns the special *our* load marker, and this function returns
  [key, b/absent]."
  [conn ks]
  (when (seq ks)
    (let [our-load-marker (new-load-marker)]
      (for [[k v] (car/wcar conn (car/lua cached-entries-script ks [our-load-marker]))]
        (list k (if (= our-load-marker v) b/absent v))))))

(defn refresh-load-markers
  "Refresh expire of load markers under keys ks, if they are still the same as we
  expect."
  [conn ^List ks ^List markers marker-fade]
  (when (seq ks)
    (.add markers marker-fade)
    (car/wcar conn
      (car/lua refresh-load-markers-script ks markers))))

(defn maintain-conn-loads
  "Maintains the list of ongoing loads. It will fetch and fill promises for entries that were
  completed by other processes. If refresh-load-markers is true, it will also extends load markers
  this JVM owns.

  This should be only called from one thread."
  [conn ^ConcurrentHashMap loads-map refresh-load-markers?]
  (let [load-marker-keys (ArrayList.)
        load-markers (ArrayList.)
        upstream-entries (ArrayList.)
        extractor (reify BiConsumer
                    (accept [this k load]
                      (if-let [marker (.getLoadMarker ^Load load)]
                        (when refresh-load-markers?
                          (.add load-marker-keys k)
                          (.add load-markers marker))
                        (.add upstream-entries k))))]
    (.forEach loads-map extractor)
    (when refresh-load-markers?
      (refresh-load-markers conn load-marker-keys load-markers load-marker-fade-sec))
    (doseq [[k v] (cached-entries conn upstream-entries)
            :let [^Load load (.remove loads-map k)]]
      (when load
        (let [p (.getPromise load)]
          (.deliver p v)
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

(defn remove-connections-without-loads
  "A function that should be called rarely, which removes connections from loading map
  which have nothing going on."
  [^ConcurrentHashMap maint-map]
  (doseq [k (into [] (.keySet maint-map))]
    (.computeIfPresent
      maint-map
      k
      (reify BiFunction
        (apply [this k v]
          (when-not (.isEmpty ^ConcurrentHashMap v) v))))))

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
  "Fetch a value, returns [present? val].
  If load-ms is positive, insert a loader marker with the specified expiry.
  Also refreshes expiry if fade is used.

  load-ms is load marker liveness in ms
  load-marker must always be provided, even if not setting it, as it's used as
  type template in an IF

  fade-ms is fade setting for entries in ms if any"
  [conn k load-marker load-ms fade-ms]
  (car/wcar conn
    (car/lua fetch-script
             {:k k}
             {:fade-ms (if fade-ms fade-ms -1)
              :load (if (and load-ms (pos-int? load-ms)) 1 0)
              :load-marker load-marker
              :load-ms load-ms})))

(def abandon-load-script (slurp (io/resource "memento/redis/poll/abandon-load.lua")))
(def finish-load-script (slurp (io/resource "memento/redis/poll/finish-load.lua")))
(def bulk-set (slurp (io/resource "memento/redis/poll/bulk-set.lua")))

(def support
  (reify LoaderSupport
    (newLoadMarker [this] (new-load-marker))
    (isLoadMarker [this o] (instance? LoadMarker o))
    (fetchEntry [this conn k load-marker load-ms fade-ms]
      (fetch conn k load-marker load-ms fade-ms))
    (completeLoad [this conn key-list v load-marker expire]
      (let [values {:load-marker load-marker :v (cval v) :ttl-ms (or expire -1)}]
        (if (= 1 (count key-list))
          (= 1 (car/wcar conn (car/lua finish-load-script {:k (first key-list)} values)))
          (let [ret (= 1 (car/wcar conn (car/lua sec-index/finish-load-script key-list values)))]
            (.put sec-index/all-indexes [conn (second key-list)] true)
            ret))))
    (abandonLoad [this conn key load-marker]
      (car/wcar conn
        (car/lua abandon-load-script {:k key} {:load-marker load-marker})))
    (completeLoadKeys [this cname kg k tag-idents]
      (if (seq tag-idents)
        (sec-index/keys-param-for-sec-idx kg k cname tag-idents)
        [k]))
    (ensureListener [this conn]
      (listener/ensure-l conn))))

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
