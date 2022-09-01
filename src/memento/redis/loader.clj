(ns memento.redis.loader
  (:require [clojure.java.io :as io]
            [memento.base :as b]
            [memento.redis.sec-index :as sec-index]
            [memento.redis.keys :as keys]
            [taoensso.nippy.tools :as nippy-tools]
            [taoensso.carmine :as car]
            [taoensso.timbre :as log])
  (:import (java.util.concurrent ConcurrentHashMap)
           (java.util.function BiFunction BiConsumer)
           (clojure.lang Keyword)
           (java.util ArrayList List UUID)))

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

(def ^ConcurrentHashMap maint
  "Map of conn to map of key to promise.

  For each connection the submap contains Redis keys to a map of:
  - :result (a promise)
  - :marker (a load marker)

  If marker is present, then it's our load and we must deliver to redis.
  If not, a foreign JVM is going to deliver to Redis, and we must scan redis for it."
  (ConcurrentHashMap. (int 4) (float 0.75) (int 8)))

(defn update-maint
  "Updates maintenance map with f. The return of the function replaces the
  entry in the maintenance map, but the function also receives a callback
  which you can call with a value and that value will be the return of the
  update-maint function. Otherwise the return is nil.

  f is a function of (fn [prev-val ret-callback])"
  [^ConcurrentHashMap maint-map conn k f]
  (let [ret (volatile! nil)
        up (reify BiFunction (apply [this k v] (f v #(vreset! ret %))))
        update-loads (reify BiFunction
                       (apply [this conn loads]
                         (let [^ConcurrentHashMap m (or loads (ConcurrentHashMap. (int 16) (float 0.75) (int 8)))]
                           (doto m (.compute k up)))))]
    (.compute maint-map conn update-loads)
    @ret))

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
                    (accept [this k {:keys [marker]}]
                      (if marker
                        (when refresh-load-markers?
                          (.add load-marker-keys k)
                          (.add load-markers marker))
                        (.add upstream-entries k))))]
    (.forEach loads-map extractor)
    (when refresh-load-markers?
      (refresh-load-markers conn load-marker-keys load-markers load-marker-fade-sec))
    (doseq [[k v] (cached-entries conn upstream-entries)]
      (.computeIfPresent loads-map k (reify BiFunction
                                       (apply [this k {:keys [result]}]
                                         (deliver result v)
                                         nil))))))

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
                         (reduce #(if (:marker (val %2)) (conj %1 (key %2)) %1) '() m))))))
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

(defprotocol Loader
  "A loader for the specific Cache"
  (start [this conn k]
    "Returns either an IDeref that returns the value of the entry, or nil if the entry is not
    being cached or calculated. It can be assumed that we claimed a load marker if nil was returned.")
  (complete [this conn k v]
    "Finishes a load with the given value, giving the entry the desired expire.")
  (put [this conn k v]
    "Pushes an entry into cache, ignoring any loading mechanism and locks."))

(defrecord PollingLoader [maint-map kg cname ttl-ms fade-ms]
  Loader
  (start [this conn k]
    (update-maint
      maint-map conn k
      (fn [entry ret-cb]
        (if entry
          ;; we're already waiting or calculating the entry, just return promise.
          (do (ret-cb (:result entry)) entry)
          (let [marker (new-load-marker)
                ;; no entry in maintenance map, let's try to claim it
                [present? cached-val] (fetch conn k marker (* 1000 load-marker-fade-sec) fade-ms)]
            (if present?
              (if (instance? LoadMarker cached-val)
                ;; someone else has a load marker, make a promise that awaits that, store it in maintenance app
                {:result (doto (promise) ret-cb)}
                ;; there's a cached value, wrap it in volatile and return nil, so no entry in maintenance map is
                ;; created
                (do (ret-cb (volatile! cached-val)) nil))
              ;; we've claimed the load
              {:result (promise) :marker marker}))))))
  (complete [this conn k v]
    (update-maint
      maint-map conn k
      (fn [entry ret-cb]
        (if-let [marker (:marker entry)]
          (let [unw-v (b/unwrap-meta v)
                values {:load-marker marker :v (cval unw-v) :ttl-ms (or ttl-ms fade-ms -1)}
                {:keys [tag-idents no-cache?]} v]
            (try
              (car/wcar conn
                (cond
                  no-cache? (car/lua abandon-load-script {:k k} (select-keys values [:load-marker]))
                  (seq tag-idents) (do (car/lua sec-index/finish-load-script (sec-index/keys-param-for-sec-idx kg k cname tag-idents) values)
                                       (.put sec-index/all-indexes [conn (keys/sec-indexes-key kg)] true))
                  :else (car/lua finish-load-script {:k k} values)))
              (catch Exception e
                (log/warn e "Error completing load to Redis for key " k)))
            (deliver (:result entry) unw-v)
            (ret-cb unw-v)
            nil)
          entry))))
  (put [this conn k v]
    (let [unw-v (b/unwrap-meta v)
          ;; don't use marker
          values {:load-marker "" :v (cval unw-v) :ttl-ms (or ttl-ms fade-ms -1)}
          {:keys [tag-idents]} v]
      (try
        (car/wcar conn
          (car/lua sec-index/finish-load-script (sec-index/keys-param-for-sec-idx kg k cname tag-idents) values)
          (when tag-idents (.put sec-index/all-indexes [conn (keys/sec-indexes-key kg)] true)))
        (catch Exception e
          (log/warn e "Error putting value to Redis for key " k)))
      nil)))

(defn if-cached
  "Retrieves an entry from redis, doing no loading at all, returning b/absent if there is no
  entry there."
  [conn k fade-ms]
  (let [[present? v] (fetch conn k (new-load-marker) -1 fade-ms)]
    (if (and present? (not (instance? LoadMarker v))) v b/absent)))

(defn for-cache
  "Return loader for RedisCache"
  [redis-cache]
  (->PollingLoader
    maint
    (-> redis-cache :fns :keygen)
    (:cname redis-cache)
    (:ttl-ms redis-cache)
    (:fade-ms redis-cache)))
