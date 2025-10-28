(ns memento.redis.listener
  (:require [memento.base :as base]
            [taoensso.carmine :as car])
  (:import (clojure.lang IFn)
           (java.io Closeable)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function BiFunction)
           (memento.base LockoutTag)))

(def ^ConcurrentHashMap invalidations
  "Invalidation ID -> [lockout-tag timestamp-ms tag-ids]. Timestamp is there so we can cull invalidations that are too long
  e.g. foreign JVM dies during invalidation, so it never triggers invalidation end."
  (ConcurrentHashMap.))

(def ^ConcurrentHashMap listeners
  "Conn -> listener"
  (ConcurrentHashMap. (int 4) (float 0.75) (int 8)))

(def channel "mem-redis-inval")

(defn listener
  "Creates a new listener. If conn is broken on-broken-listener is called"
  [conn-spec f ^Runnable on-broken-listener]
  (car/with-new-listener conn-spec
    (fn [[typ c msg] _]
      (case typ
        "message" (when (= c channel) (f msg))
        "carmine" (when (and (= "carmine:listener:error" c) (= :conn-broken (:error msg)))
                    (on-broken-listener))
        nil)) {}
    (car/subscribe channel)))

;; on duplicates: it is possible that the user has multiple connections that actually map to the same
;; redis instance, e.g. {} and {:timeout 500} are same connection logically, but our mechanism here will
;; think its different


(defn process-msg [[action invalidation-id items]]
  (case action
    :start (let [tag (LockoutTag. invalidation-id)]
             ;; we need to insert before starting lockout, so the listener can detect
             ;; if we it was this .startLockout that caused the event or not
             (when-not (.putIfAbsent invalidations invalidation-id [tag (System/currentTimeMillis) items])
               (.startLockout base/lockout-map items tag)))
    ;; we remove first, so the listener can detect that it was this function that caused
    ;; .endLockout event
    :end (when-let [[tag _ items] (.remove invalidations invalidation-id)]
           (.endLockout base/lockout-map items tag))
    nil)
  nil)

(defn event-start [^LockoutTag lockout-tag items]
  ;; if we already had an invalidation in a map, then this event was caused
  ;; process-msg above (or by duplicates, see above)
  (when-not (.putIfAbsent invalidations (.getId lockout-tag) [lockout-tag Long/MAX_VALUE items])
    ;; otherwise post to remote that we started a lockout
    (run! (fn [conn]
            (car/wcar conn
              (car/publish channel [:start (.getId lockout-tag) items])))
          (keys listeners))))

(defn event-end [^LockoutTag lockout-tag]
  ;; if this event was caused by process-msg above then the invalidation will have been removed already
  ;; so ignore that case.
  (try
    (when (.remove invalidations (.getId lockout-tag))
      (run! (fn [conn]
              (car/wcar conn
                (car/publish channel [:end (.getId lockout-tag)])))
            (keys listeners)))
    (catch Exception e
      (.printStackTrace e))))

(defn ensure-l [conn]
  ((reify
     IFn
     ;; add new listener if one is not running, see below
     (invoke [this] (.compute listeners (:spec conn {}) this))
     (invoke [this k v]
       (if (not= (some-> (:status_ v) deref) :running)
         (listener k process-msg this)
         v))
     BiFunction
     (apply [this k v]
       ;; here we benefit from the swapping BiFunction also being a on-broken-listener function
       (if (not= (some-> (:status_ v) deref) :running)
         (listener k process-msg this)
         v)))))

(defn shutdown-listeners []
  (run! #(.close ^Closeable %) (vals listeners))
  (.clear listeners))

(defn remove-old-invalidations [interval]
  (let [ts (- (System/currentTimeMillis) interval)]
    (.replaceAll invalidations
       (reify BiFunction
         (apply [this k [tag time items :as v]]
           (if (< time ts)
             (do (.endLockout base/lockout-map items tag)
                 nil)
             v))))))

(.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable shutdown-listeners))