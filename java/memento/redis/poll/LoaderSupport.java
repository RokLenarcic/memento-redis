package memento.redis.poll;

import clojure.lang.IPersistentSet;
import clojure.lang.IPersistentVector;

import java.util.List;

public interface LoaderSupport {

    /**
     * {@link #completeLoad} return code: the value was written to Redis successfully.
     * Emitted by both finish-load.lua and finish-load-w-sec.lua.
     */
    int COMPLETE_LOAD_STORED = 1;

    /**
     * {@link #completeLoad} return code: nothing was written because the load marker
     * stored under the key no longer matches ours. Another loader (or an invalidation)
     * raced ahead and cleared/replaced our claim. The caller's in-flight value is still
     * valid for the joiners that already attached to the promise; we just don't
     * publish it to Redis. Emitted by both finish-load.lua and finish-load-w-sec.lua.
     */
    int COMPLETE_LOAD_MARKER_MISMATCH = 0;

    /**
     * {@link #completeLoad} return code (tagged variant only): one of the tags we
     * depended on was invalidated between {@link #fetchEntry} (which captured
     * the validation epoch) and finalization. The script removed our load marker
     * from Redis. The caller must reject the in-JVM promise so joiners re-loop.
     * Emitted only by finish-load-w-sec.lua.
     */
    int COMPLETE_LOAD_STALE_TAG = -1;

    IPersistentVector fetchEntry(Object conn, Object k, Load load, Object loadMs, Object fadeMs);

    IPersistentVector fetchCachedEntry(Object conn, Object cacheName, Object keysGenerator, Object k, Object fadeMs);

    int completeLoad(Object conn, IPersistentVector keys, Object value, Load load, Long expire);

    void putValue(Object conn, Object cacheName, Object keysGenerator, Object key, Object value, Long expire);

    void putValues(Object conn, List<Object> keys, List<Object> values, Long expire);

    void abandonLoad(Object conn, Object key, Load load);

    IPersistentVector completeLoadKeys(Object cacheName, Object keysGenerator, Object key, IPersistentSet tagIdents);

    void ensureListener(Object conn);

    /**
     * Issue a {@code DEL key} against {@code conn}. Used by the corrupt-
     * discriminator path in {@link Loader#get}'s hit branch for livelock
     * prevention (§5.4.2).
     */
    void delEntry(Object conn, Object key);
}
