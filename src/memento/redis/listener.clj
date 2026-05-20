(ns memento.redis.listener
  (:require [taoensso.carmine :as car])
  (:import (clojure.lang IFn)
           (java.io Closeable)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function BiFunction)
           (memento.base TagInvalidation)))

(def ^ConcurrentHashMap invalidations
  "Invalidation epoch -> [timestamp-ms tag-ids]. Timestamp is there so we can cull invalidations that are too long
  e.g. foreign JVM dies during invalidation, so it never triggers invalidation end."
  (ConcurrentHashMap.))

(def ^ConcurrentHashMap listeners
  "Conn -> listener"
  (ConcurrentHashMap. (int 4) (float 0.75) (int 8)))

(def channel "mem-redis-inval")

(def tag-invalidation TagInvalidation/INSTANCE)

(defn listener
  "Creates a new listener. If conn is broken on-broken-listener is called"
  [conn f ^Runnable on-broken-listener]
  (let [conn-spec (:spec conn)]
    (car/with-new-listener conn-spec
      (fn [[typ c msg] _]
        (case typ
          "message" (when (= c channel) (f msg))
          "carmine" (when (and (= "carmine:listener:error" c) (= :conn-broken (:error msg)))
                      (on-broken-listener))
          nil)) {}
      (car/subscribe channel))))

;; on duplicates: it is possible that the user has multiple connections that actually map to the same
;; redis instance, e.g. {} and {:timeout 500} are same connection logically, but our mechanism here will
;; think its different


(defn process-msg [[action epoch items]]
  (case action
    :start (when-not (.putIfAbsent invalidations epoch [(System/currentTimeMillis) items])
             (.startInvalidation tag-invalidation items epoch))
    ;; we remove first, so the listener can detect that it was this function that caused
    ;; .endInvalidation event
    :end (when-let [[_ items] (.remove invalidations epoch)]
           (.endInvalidation tag-invalidation items epoch))
    nil)
  nil)

(defn event-start [items epoch-map]
  ;; if we already had an invalidation in a map, then this event was caused
  ;; process-msg above (or by duplicates, see above)
  (doseq [item items
          :let [epoch (get epoch-map item)]
          :when epoch]
    (when-not (.putIfAbsent invalidations epoch [Long/MAX_VALUE items])
      ;; otherwise post to remote that we started an invalidation
      (run! (fn [conn]
              (car/wcar conn
                (car/publish channel [:start epoch items])))
            (keys listeners)))))

(defn event-end [items epoch-map]
  ;; if this event was caused by process-msg above then the invalidation will have been removed already
  ;; so ignore that case.
  (try
    (doseq [epoch (or (seq (keep #(get epoch-map %) items))
                       (seq (keep (fn [[epoch [_ stored-items]]]
                                    (when (= items stored-items) epoch))
                                  (seq invalidations))))]
      (when (.remove invalidations epoch)
        (run! (fn [conn]
                (car/wcar conn
                  (car/publish channel [:end epoch])))
              (keys listeners))))
    (catch Exception e
      (.printStackTrace e))))

(defn ensure-l [conn]
  ((reify
     IFn
     ;; add new listener if one is not running, see below
     (invoke [this] (.compute listeners conn this))
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
    (doseq [[epoch [time items :as v]] (seq invalidations)
            :when (< time ts)]
      (when (.remove invalidations epoch v)
        (.endInvalidation tag-invalidation items epoch)))))

(.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable shutdown-listeners))
