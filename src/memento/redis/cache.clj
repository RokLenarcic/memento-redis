(ns memento.redis.cache
  (:require
    [memento.base :as b]
    [memento.config :as mc]
    [memento.redis.keys :as keys]
    [memento.redis.loader :as loader]
    [memento.redis.poll.daemon :as daemon]
    [memento.redis.sec-index :as sec-index]
    [memento.redis.util :as util]
    [taoensso.carmine :as car])
  (:import (clojure.lang IDeref)
           (memento.base EntryMeta ICache Segment)
           (memento.redis.poll Loader)))

(defrecord RedisCache [conf fns cname ^Loader lookup]
  ICache
  (conf [this] conf)
  (cached [this segment args]
    (let [{:keys [conn key-fn]} fns
          k (key-fn segment args)
          c (conn)]
      (loop []
        (let [ret (.get lookup c segment args k)]
          (if (identical? EntryMeta/absent ret) (recur) ret)))))
  (ifCached [this segment args]
    (let [{:keys [conn key-fn]} fns]
      (.ifCached lookup (conn) segment (key-fn segment args))))
  (invalidate [this segment]
    (let [{:keys [conn keygen]} fns
          c (conn)]
      (.invalidateByPred lookup c #(keys/segment-key? keygen cname segment %))
      (util/del-keys-by-pattern c (keys/segment-wildcard-key keygen cname segment))))
  (invalidate [this segment args]
    (let [{:keys [conn key-fn]} fns
          c (conn)
          k (key-fn segment args)]
      (.invalidate lookup c k)
      (car/wcar c (car/del k))
      this))
  (invalidateAll [this]
    (let [{:keys [conn keygen]} fns
          c (conn)]
      (.invalidateByPred lookup c #(keys/cache-key? keygen cname %))
      (util/del-keys-by-pattern c (keys/cache-wildcard-key keygen cname))
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
                                   (do (.putValue lookup c segment ckey cval) coll)
                                   (conj coll [ckey cval]))))
                             [])
                           car/return
                           (car/wcar c))
            by-expiry (group-by (fn [[k v]] (.expiryMs lookup segment k v)) remaining)]
        ;; those without secondary indexes we can push freely
        (doseq [[expiry items] by-expiry
                batch (partition-all 100 items)]
          (car/wcar c
            (car/lua loader/bulk-set
                     (mapv first batch)
                     (conj (mapv second batch) (or expiry -1))))))))
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
                nil)]
    (deref daemon/daemon-thread)
    (assoc cache :lookup (loader/for-conf conf (-> cache :fns :keygen)))))
