(ns memento.redis
  "Settings for Redis cache.

  Besides there, the cache also supports these settings from
  base Memento:
  - key-fn
  - ret-fn
  - evt-fn
  - ttl
  - fade"
  (:require [memento.core :as core]
            [memento.redis.cache :as cache]
            [memento.redis.util :as util]
            [memento.redis.keys :as keys])
  (:refer-clojure :exclude [type name])
  (:import (memento.redis.cache RedisCache)))

(def cache
  "Value for :memento.core/type setting for redis cache"
  ::cache)

(def name
  "Cache name. It is a part of Redis keys' names.

  It is useful if you want to cache the same function in two different caches and
  have them not collide.

  It defaults to empty string."
  ::name)

(def conn
  "Redis connection setting. It can be:
  - a Carmine connection map
  - a IDeref object (Var, Atom) which will contain a Carmine connection map, it
  will be derefed at each use.
  - a 0-arg fn that will return a Carmine connection map,
  it will be evaluated at each use

  2 of 3 here will be evaluated each lookup (or other operation)."
  ::conn)

(defn sha256
  "Function that returns a byte array with SHA-256 hash of the Nippy serialized
  form of the object. Used in conjunction with key-fn to shorten long keys.

  Since serialized function arguments are part of the Redis keys,
  this can lead to functions with large arguments using up a lot of
  RAM in Redis just due to keys.

  You can write a key-fn that returns a hash of the actual function arguments (or a particular long argument).

  This is a convenience function to achieve that.

  Note that nippy already does lz4 compression on keys over 8kb."
  [o]
  (keys/digest :sha-256 o))

(defn sha1
  "Function that returns a byte array with SHA1 hash of the Nippy serialized
  form of the object. Used in conjunction with key-fn to shorten long keys.

  Since serialized function arguments are part of the Redis keys,
  this can lead to functions with large arguments using up a lot of
  RAM in Redis just due to keys.

  You can write a key-fn that returns a hash of the actual function arguments (or a particular long argument).

  This is a convenience function to achieve that.

  Note that nippy already does lz4 compression on keys over 8kb."
  [o]
  (keys/digest :sha1 o))

(def keygen
  "Specify the way the keys are generated. Value is an instance of KeysGenerator protocol.

  If not specified, the default keys generator will be used."
  ::keygen)

(def anon-key
  "Specifies how Memento Segment ID is processed by default KeysGenerator, when ID is a function object.
  Available settings: :empty, :stringify, defaults to :empty.

  If :empty, then any function object ID is replaced by empty string.
  If :stringify, then any function object ID is replaced by (str function)"
  ::anon-key)

(defn nuke!!
  "Remove all entries created by Memento.

  The parameter is a memoized function.

  The actual database being nuked (connection and database) and key naming schema (KeysGenerator)
  is determined based on the settings on the memoized function. For most applications pointing this
  at any memoized function will have the same result, but if you're using multiple different
  databases or KeysGenerator instances, then only the corresponding database/keys schema is wiped."
  ([memoized-function]
   (let [cache (core/active-cache memoized-function)]
     (when (instance? RedisCache cache)
       (nuke!! (-> cache :fns :conn) (-> cache :fns :keygen)))))
  ([conn keys-generator]
   (util/nuke-keyspace ((cache/conf-conn {::conn conn})) keys-generator)))

(def hit-detect?
  "Cache setting, if set to true, any time there's a cache miss and an IObj is returned, it will have
  meta key :memento.redis/cached? with value true/false"
  ::hit-detect?)
