package memento.redis.poll;

import memento.base.LockoutMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Loads {

    /**
     * How long before a load marker fades. This is to prevent JVM exiting or dying from leaving LoadMarkers
     * in Redis indefinitely, causing everyone to block on that key forever. This time is refreshed every
     * second by a daemon thread, however a long GC will cause LoadMarkers to fade when they shouldn't.
     *
     *   Adjust this setting appropriately via memento.redis.load_marker_fade system property.
     *
     */
    public static final int LOAD_MARKER_FADE_SEC = Integer.parseInt(System.getProperty("memento.redis.load_marker_fade", "5"));



    /**
     * Map of conn to map of key to promise.
     *
     *   For each connection the submap contains Redis keys to a map of:
     *   - :result (a promise)
     *   - :marker (a load marker)
     *
     *   If marker is present, then it's our load and we must deliver to redis.
     *   If not, a foreign JVM is going to deliver to Redis, and we must scan redis for it.
     */
    public static final ConcurrentHashMap<Object, ConcurrentHashMap<Object, Load>> maint = new ConcurrentHashMap<>(4, 0.75f, 8);

    static {
        LockoutMap.INSTANCE.addListener(
                new LockoutMap.Listener() {
                    @Override
                    public void startLockout(Iterable<Object> iterable, CountDownLatch countDownLatch) {

                    }

                    @Override
                    public void endLockout(Iterable<Object> iterable, CountDownLatch countDownLatch) {
                        for (ConcurrentHashMap<Object, Load> m : maint.values()) {
                            for (Load l : m.values()) {
                                l.getPromise().addInvalidIds(iterable);
                            }
                        }
                    }
                }
        );
    }
}

