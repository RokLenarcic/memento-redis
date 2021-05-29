# Memento Redis

Memento cache backed by Redis. The underlying library is Carmine (refer to their documentation if you're interested in implementation details).

Offers **guarantees that a cache entry is calculated only once even if using multiple JVMs**.

## Usage

Use this library by invoking Memento functions with a new cache type and properties.

All the relevant code is in `memento.redis` namespace. See docstrings.

```clojure
(ns example
  (:require [memento.core :as m]
            [memento.config :as mc]
            [memento.redis :as mr]))

(defn my-function [x]
  (* 2 x))

(m/memo #'my-function {mc/type mr/cache mr/conn {}})
```

Creates an infinite cache for function `my-function` in Redis with Carmine connection `{}` (localhost).

## Connection configuration

You must provide a Carmine connection in your cache configuration via key `memento.redis/cache`.

There are multiple forms that this cache parameter can take:
- a map
- an IDeref (atom, var, delay, volatile, ) that will return Carmine connection map
- a fn of 0 args that will return a Carmine connection map

The latter two will be queried for connection map each time connection is used.

## Time based eviction

Use normal Memento TTL and Fade settings. Do not define both settings on same cache, because
Redis EXPIRE mechanism cannot support both modes at once.

## Size based eviction

Redis itself does not support size based eviction except when defined over whole databases. They suggest that you create a redis
instance for each such scenario.

[**Using Redis as an LRU cache**](https://redis.io/topics/lru-cache)

## Cache name

Cache can have a name, specified by `:memento.redis/name` cache conf property. It defaults to `""`, which might can be
ok for most caches.

If use multiple Caches to cache the same function (e.g. by swapping cache using 
`memento.core/with-caches` or similar mechanism), you need to give a different name to at least one of them, otherwise
they will both see same entries. So any sort of scoped cache should provide a name.

All caches having same name (`""`) also affects Memento core function `m/memo-clear-cache!`, it will clear entries for
all caches with same name.

## Redis key strategy

**The most important setting when using this cache.** A single Memento Redis cache can contain entries for
multiple functions, but when stored in Redis, this structure is represented by flat string keys. **It is important how these keys are kept from colliding.**

Keys are derived from triplets:
- Memento Redis cache name (set by :memento.redis/name cache configuration property)
- Memento Segment (the cached function MountPoint)
- argument list of the function call

You can configure how Memento segment part of this triplet is handled. 

By default, the segment ID is used as part of the Redis key to disambiguate entries from different MountPoints (cached functions).

You can provide ID for the function's Cache Segment when you bind a function cache using `:memento.core/id` setting:

```clojure
(m/memo #'my-function {mc/type mr/cache mr/conn {} mc/id `my-function})
```

**It is incredibly important if you're memoizing anonymous functions that close over values in scope, to make those
values part of the ID.**

```clojure
(defn adder [x]
  (m/memo (fn [y] (+ x y)) {mc/type mr/cache mr/conn {} mc/id [`adder x]}))
```

Now the memoized function will correctly separate the entries generated by `((adder 3) 4)` and `((adder 4) 4)`,
because `[my-namespace/adder 3]` is now part of the cache key.

The ID can be of any type. If the ID is not specified, then it defaults:
- String qualified name of Var if `memo` is called on a Var
- Java function object if you call `memo` directly on a function 

It is this second scenario that is problematic, and Memento Redis has a couple of options for dealing with anonymous functions' Segments.

### 1. Provide ID when memoizing non-vars

Avoid the problem by always providing the ID via `:memento.core/id` option when memoizing
functions.

### 2. The :stringify option

If you don't configure anything, Memento Redis will make parts of Redis keys that derive from Segment ID be 
`str`ed when it is a function object (as if you specified `:memento.redis/anon-key` `:stringify` in your cache config).

This will solve any sort of problems with entries from different anonymous functions colliding with each other, but 
there is a huge downside: when you restart the JVM (or even reload the function's namespace in REPL),
you will get a new Function object from same code with a different `str` representation. This will effectively cause
the cache to start over as empty for this function, and any entries from previous session will stay in Redis until
their expiry (or you can invalidate them manually).

You can try manually invalidating the cache to prevent accumulation of dead entries:
```clojure
(m/memo-clear-cache! (m/active-cache my-function))
```
Or use the nuke option at the bottom of README.

### 3. The :empty option

Alternatively you can specify `:memento.redis/anon-key` cache setting as `:empty` and Memento Redis will make 
parts of Redis keys that derive from Segment ID be always `nil` when it is a function object.

This will prevent problems described above where anonymous functions lose their cache on JVM restarts and namespace
reloads, but there's a downside: multiple anonymous functions can now collide keys in Redis.

This is fine if:
- you're caching one function per Cache, and you are specifying Cache name on each of them

or
- you're memoizing Vars and/or providing `:memento.core/id` setting

or
- you are caching just a few functions, and their argument lists are different enough to not collide
  Redis keys (different count or types of arguments)

or some combination of the above.

### 4. Write your own KeysGenerator

You can write an entirely custom strategy for naming Redis keys for your cache entries. You need to implement
`memento.redis.keys/KeysGenerator` protocol and provide such object to your cache via setting `:memento.redis/keygen`.

If you do so, everything described above doesn't apply, but I still suggest you take a look at the existing implementation
of the default KeysGenerator.

## Compression

Carmine already does LZ4 compression automatically for keys and values over 8KB.

## Compressing your keys further with hashing

If your functions' arguments are really large, you might want to use hashed keys. For example, you might have a function
where the parameter is a big JSON, and the output is an HTML render.

You can achieve that by using the normal Memento `key-fn` cache option (or `comp` on existing one) with one of the convenience functions:
- memento.redis/sha1
- memento.redis/sha256

```clojure
(def inf-cache {mc/type mr/cache mr/conn {} mc/key-fn mr/sha1})

(m/memo #'my-function inf-cache)
```

## Removing all Memento Redis related keys from Redis

When, for one reason or another, you've got cache related keys stuck in Redis, and you
just want to purge everything.

```clojure
(mr/nuke!! my-function)
```

Call it on a memoized function, and it will delete all Memento Redis keys on the connection
that memoized function is using (and the key generator). If you are using multiple
connections and/or key generators, you need multiple call with the correct functions.

## Futures and thread-pools

This library uses `future` and you might need to call `shutdown-agents` to
make JVM shutdown promptly.

## License

Copyright © 2021 Rok Lenarčič

Licensed under the term of the Eclipse Public License - v 2.0, see LICENSE.
