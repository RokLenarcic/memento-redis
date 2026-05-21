package memento.redis.poll;

import clojure.lang.IPersistentSet;
import clojure.lang.IPersistentVector;

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

    /**
     * Returns new LoadMarker
     * @return
     */
    Object newLoadMarker();
    boolean isLoadMarker(Object o);
    IPersistentVector fetchEntry(Object conn, Object cacheName, Object keysGenerator, Object k, Object loadMarker, Object loadMs, Object fadeMs);

    int completeLoad(Object conn, IPersistentVector keys, Object val, Object loadMarker, Long expire, Long validationEpoch);

    void abandonLoad(Object conn, Object key, Object loadMarker);

    IPersistentVector completeLoadKeys(Object cacheName, Object keysGenerator, Object key, IPersistentSet tagIdents);

    void ensureListener(Object conn);
}
