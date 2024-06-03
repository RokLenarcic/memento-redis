package memento.redis.poll;

import clojure.lang.IPersistentSet;
import clojure.lang.IPersistentVector;

public interface LoaderSupport {

    /**
     * Returns new LoadMarker
     * @return
     */
    Object newLoadMarker();
    boolean isLoadMarker(Object o);
    IPersistentVector fetchEntry(Object conn, Object k, Object loadMarker, Object loadMs, Object fadeMs);

    boolean completeLoad(Object conn, IPersistentVector keys, Object val, Object loadMarker, Long expire);

    void abandonLoad(Object conn, Object key, Object loadMarker);

    IPersistentVector completeLoadKeys(Object cacheName, Object keysGenerator, Object key, IPersistentSet tagIdents);

    void ensureListener(Object conn);
}
