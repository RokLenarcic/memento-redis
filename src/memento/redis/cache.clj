(ns memento.redis.cache
  (:require
    [memento.base :as b]
    [memento.core :as c]
    [memento.config :as mc]
    [memento.redis.keys :as keys]
    [memento.redis.loader :as loader]
    [memento.redis.poll.daemon :as daemon]
    [memento.redis.sec-index :as sec-index]
    [memento.redis.util :as util]
    [taoensso.carmine :as car])
  (:import (clojure.lang AFn IDeref IObj)
           (memento.base EntryMeta ICache Segment)))

(defn wrap-hits [conf obj hit?]
  (if (and (:memento.redis/hit-detect? conf) (instance? IObj obj))
    (vary-meta obj assoc :memento.redis/cached? hit?)
    obj))

(defrecord RedisCache [conf fns cname ttl-ms fade-ms lookup]
  ICache
  (conf [this] conf)
  (cached [this segment args]
    (let [{:keys [conn key-fn ret-fn]} fns
          k (key-fn segment args)
          c (conn)]
      (if-some [maintenance-data (loader/start lookup c k)]
        (let [ret @maintenance-data]
          (if (= ret b/absent)                              ; failed load
            (recur segment args)
            (wrap-hits conf ret true)))
        (try
          (let [f (if ret-fn (fn [& args] (ret-fn args (apply (.getF ^Segment segment) args)))
                             (.getF ^Segment segment))
                calculated (AFn/applyToHelper f args)]
            (wrap-hits conf (loader/complete lookup c k calculated) false))
          (catch Exception e
            (loader/complete lookup c k (c/do-not-cache b/absent))
            (throw e))))))
  (ifCached [this segment args]
    (let [{:keys [conn key-fn]} fns]
      (loader/if-cached (conn) (key-fn segment args) fade-ms)))
  (invalidate [this segment]
    (let [{:keys [conn keygen]} fns]
      (util/del-keys-by-pattern
        (conn)
        (keys/segment-wildcard-key keygen cname segment))))
  (invalidate [this segment args]
    (let [{:keys [conn key-fn]} fns]
      (car/wcar (conn) (car/del (key-fn segment args)))
      this))
  (invalidateAll [this]
    (let [{:keys [conn keygen]} fns]
      (util/del-keys-by-pattern
        (conn)
        (keys/cache-wildcard-key keygen cname))
      this))
  (invalidateIds [this ids]
    (let [{:keys [conn keygen]} fns]
      (sec-index/invalidate-by-index
        (conn)
        (keys/sec-indexes-key keygen)
        (mapv #(keys/sec-index-id-key keygen cname %) ids))
      this))
  (addEntries [this segment args-to-vals]
    (when (seq args-to-vals)
      (let [{:keys [key-fn conn]} fns
            c (conn)
            ;; collect all kv-s without secondary index to bulk push them, but run secondary index
            ;; ones one by one
            remaining (->> args-to-vals
                           (reduce-kv
                             (fn [coll k v]
                               (let [cval (loader/cval v) ckey (key-fn segment k)]
                                 (if (and (instance? EntryMeta v) (.getTagIdents ^EntryMeta v))
                                   (do (loader/put lookup c ckey cval) coll)
                                   (conj coll [ckey cval]))))
                             [])
                           car/return
                           (car/wcar c))]
        ;; those without secondary indexes we can push freely
        (doseq [batch (partition-all 100 remaining)]
          (car/wcar c
            (car/lua loader/bulk-set
                     (mapv first batch)
                     (conj (mapv second batch) (or ttl-ms fade-ms -1))))))))
  (asMap [this]
    (let [{:keys [conn keygen]} fns]
      (util/kv-by-pattern
        (conn)
        (keys/cache-wildcard-key keygen cname)
        #(next (next %)))))
  (asMap [this segment]
    (let [{:keys [conn keygen]} fns]
      (util/kv-by-pattern
        (conn)
        (keys/segment-wildcard-key keygen cname segment)
        #(last %)))))

(defn conf-cache-name [conf]
  (:memento.redis/name conf ""))

(defn conf-conn [conf]
  (let [c (:memento.redis/conn conf)]
    (cond
      (nil? c) (throw (ex-info "Missing configuration key: Redis connection" {:key :memento.redis/conn}))
      (instance? IDeref c) #(deref c)
      (map? c) (constantly c)
      (ifn? c) c
      :else (throw (ex-info "Wrong configuration key: Redis connection value is not one of the supported types"
                            {:key :memento.redis/conn
                             :val c})))))

(defn conf-keygen [conf]
  (if-let [kg (:memento.redis/keygen conf)]
    (if (satisfies? keys/KeysGenerator kg)
      kg
      (throw (ex-info "Wrong configuration key: Keygen value is not one of the supported types"
                      {:key :memento.redis/keygen
                       :val kg})))
    (keys/default-keys-generator "M^" "MS^" (:memento.redis/anon-key conf :stringify))))

(defn conf-key-fn [kg conf]
  (let [cache-name (conf-cache-name conf)]
    (if-let [key-fn (mc/key-fn conf)]
      (fn [^Segment segment args]
        (keys/entry-key kg cache-name segment (key-fn ((.getKeyFn segment) (seq args)))))
      (fn [^Segment segment args]
        (keys/entry-key kg cache-name segment ((.getKeyFn segment) (seq args)))))))

(defn functions [conf]
  (let [kg (conf-keygen conf)]
    {:conn (conf-conn conf)
     :key-fn (conf-key-fn kg conf)
     :ret-fn (mc/ret-fn conf)
     :keygen kg}))

(defn conf-millis [conf k]
  (when-let [v (k conf)]
    (.toMillis (b/parse-time-unit v) (b/parse-time-scalar v))))

(defn assert-no-size [conf]
  (when (mc/size< conf)
    (throw (ex-info "Wrong configuration key: size based eviction not supported"
                    {:key mc/size<
                     :val (mc/size< conf)}))))

(defn assert-not-both-fade-ttl
  [conf]
  (when (every? conf [mc/ttl mc/fade])
    (throw (ex-info "Wrong configuration key combination: having both Fade and TTL setting is not allowed."
                    {:key [mc/ttl mc/fade]
                     :val (select-keys conf [mc/ttl mc/fade])}))))

(defmethod b/new-cache :memento.redis/cache [conf]
  (assert-no-size conf)
  (assert-not-both-fade-ttl conf)
  (let [cache (->RedisCache
                conf
                (functions conf)
                (conf-cache-name conf)
                (conf-millis conf mc/ttl)
                (conf-millis conf mc/fade)
                nil)]
    (deref daemon/daemon-thread)
    (assoc cache :lookup (loader/for-cache cache))))
