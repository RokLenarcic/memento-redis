package memento.redis.poll;

import clojure.lang.*;
import memento.base.Durations;
import memento.base.EntryMeta;
import memento.base.LockoutMap;
import memento.base.Segment;
import memento.caffeine.SpecialPromise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Loader {

    /**
     * How long before a load marker fades. This is to prevent JVM exiting or dying from leaving LoadMarkers
     * in Redis indefinitely, causing everyone to block on that key forever. This time is refreshed every
     * second by a daemon thread, however a long GC will cause LoadMarkers to fade when they shouldn't.
     *
     *   Adjust this setting appropriately via memento.redis.load_marker_fade system property.
     *
     */
    public static final int LOAD_MARKER_FADE_SEC = Integer.parseInt(System.getProperty("memento.redis.load_marker_fade", "5"));


    private final Object cacheName;
    private final Object keysGenerator;

    private final IFn retFn;
    private final IFn retExFn;

    private final Object ttl;
    private final Object fade;

    private final IFn ttlFn;

    private final boolean doHitDetection;

    private final ConcurrentHashMap<Object, ConcurrentHashMap<Object, Load>> maint;

    public Loader(Object cacheName, Object keysGenerator, IFn retFn, IFn retExFn, Object ttl, Object fade, IFn ttlFn, boolean doHitDetection, ConcurrentHashMap<Object, ConcurrentHashMap<Object, Load>> maint, LoaderSupport s) {
        this.cacheName = cacheName;
        this.keysGenerator = keysGenerator;
        this.retFn = retFn;
        this.retExFn = retExFn;
        this.ttl = ttl;
        this.fade = fade;
        this.ttlFn = ttlFn;
        this.doHitDetection = doHitDetection;
        this.maint = maint;
        this.s = s;
    }

    private final LoaderSupport s;

    private IPersistentVector completeLoadKeys(Object k, IPersistentSet tagIdents) {

        return s.completeLoadKeys(cacheName, keysGenerator, k, tagIdents);
    }

    public ConcurrentHashMap<Object, Load> connMap(Object conn) {
        return maint.computeIfAbsent(conn, x -> {
            s.ensureListener(conn);
            return new ConcurrentHashMap<>(16, 0.75f, 8);
        });
    }


    public void putValue(Object conn, Segment segment, Object k, Object v) {
        // don't use marker
        Object loadMarker = "";
        Long writeExpiry = expiryMs(segment, k, v);
        if (v instanceof EntryMeta) {
            EntryMeta em = (EntryMeta) v;
            s.completeLoad(conn, completeLoadKeys(k, em.getTagIdents()), em.getV(), loadMarker, writeExpiry);
        } else {
            s.completeLoad(conn, completeLoadKeys(k, null), v, loadMarker, writeExpiry);
        }
    }

    private Long fadeMs(Segment segment) {
        IPersistentMap segmentConf = segment.getConf();
        IFn ttlFn = (IFn) segmentConf.valAt(ttlFnKw);
        Object fade = segmentConf.valAt(Durations.fadeKw);
        Object ttl = segmentConf.valAt(Durations.ttlKw);
        Object ret;
        if (ttlFn == null && fade == null && ttl == null) {
            ret = this.fade;
        } else {
            ret = fade;
        }
        return ret == null ? null : Durations.millis(ret);
    }

    private static final Keyword ttlFnKw = Keyword.intern("memento.redis", "ttl-fn");
    private static final Keyword cachedKw = Keyword.intern("memento.redis", "cached?");

    public Long expiryMs(Segment segment, Object key, Object v) {
        IPersistentMap segmentConf = segment.getConf();

        IFn ttlFn = (IFn) segmentConf.valAt(ttlFnKw);
        Object fade = segmentConf.valAt(Durations.fadeKw);
        Object ttl = segmentConf.valAt(Durations.ttlKw);
        if (ttlFn == null && fade == null && ttl == null) {
            ttlFn = this.ttlFn;
            fade = this.fade;
            ttl = this.ttl;
        }
        Object ret = null;
        if (ttlFn != null) {
            ret = ttlFn.invoke(segment, key, v);
        }
        if (ret == null) {
            ret = ttl;
            if (ret == null) {
                ret = fade;
            }
        }
        return ret == null ? null : Durations.millis(ret);
    }

    private Object markCachedMeta(IObj o) {
        IPersistentMap meta = o.meta();
        return o.withMeta(meta == null ? new PersistentArrayMap(new Object[]{cachedKw, true}) : meta.assoc(cachedKw, true));
    }

    private Object markCached(Object o) {
        if (doHitDetection) {
            if (o instanceof IObj) {
                return markCachedMeta((IObj) o);
            } else if (o instanceof EntryMeta) {
                EntryMeta em = (EntryMeta) o;
                if (em.getV() instanceof IObj) {
                    return new EntryMeta(markCachedMeta((IObj)em.getV()), em.isNoCache(), em.getTagIdents());
                }
            }
        }
        return o;
    }

    private Object await(Object key, Load l) throws Throwable {
        SpecialPromise p = l.getPromise();
        Object ret = p.await(key);
        if (ret != EntryMeta.absent && !LockoutMap.awaitLockout(ret)) {
            // if not invalidated, return the value
            return EntryMeta.unwrap(ret);
        } else {
            return EntryMeta.absent;
        }
    }

    /**
     * if this returns absent you need to loop
     * @param conn
     * @param segment
     * @param args
     * @return
     * @throws Throwable
     */
    public Object get(Object conn, Segment segment, ISeq args, Object key) throws Throwable {
        Load newLoad = new Load(s.newLoadMarker());
        ConcurrentHashMap<Object, Load> loads = connMap(conn);
        Load prevLoad = loads.putIfAbsent(key, newLoad);
        if (prevLoad == null) {
            SpecialPromise p = newLoad.getPromise();
            try {
                Long fadeMs = fadeMs(segment);
                // no entry in maintenance map, let's try to claim it in Redis
                IPersistentVector entry = s.fetchEntry(conn, key, newLoad.getLoadMarker(), LOAD_MARKER_FADE_SEC * 1000, fadeMs);
                // entry already exists in Redis
                if (entry.valAt(0) != null) {
                    Object cachedVal = entry.valAt(1);
                    if (s.isLoadMarker(cachedVal)) {
                        // someone else has a load marker, mark it and wait for maintenance to fill it in;
                        newLoad.foreignLoad();
                        return await(key, newLoad);
                    } else {
                        // there's a cached value, remove the load and return it if valid, otherwise absent
                        loads.remove(key, newLoad);
                        cachedVal = markCached(cachedVal);
                        return p.deliver(cachedVal) ? EntryMeta.unwrap(cachedVal) : EntryMeta.absent;
                    }
                } else {
                    newLoad.ourLoad();
                    Object result = AFn.applyToHelper(segment.getF(), args);
                    if (retFn != null) {
                        result = retFn.invoke(args, result);
                    }
                    loads.remove(key, newLoad);
                    if (!p.deliver(result)) {
                        // The SpecialPromise was invalidated
                        s.abandonLoad(conn, key, newLoad.getLoadMarker());
                        return EntryMeta.absent;
                    }
                    Long writeExpiry = expiryMs(segment, key, result);
                    if (result instanceof EntryMeta) {
                        EntryMeta em = (EntryMeta) result;
                        if (em.isNoCache()) {
                            s.abandonLoad(conn, key, newLoad.getLoadMarker());
                        } else {
                            s.completeLoad(conn, completeLoadKeys(key, em.getTagIdents()), result, newLoad.getLoadMarker(), writeExpiry);
                        }
                        return em.getV();
                    } else {
                        s.completeLoad(conn, completeLoadKeys(key, null), result, newLoad.getLoadMarker(), writeExpiry);
                        return result;
                    }
                }
            } catch (Throwable t) {
                loads.remove(key, newLoad);
                try {
                    s.abandonLoad(conn, key, newLoad.getLoadMarker());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (!p.isInvalid()) {
                    p.deliverException(retExFn == null ? t : (Throwable) retExFn.invoke(args, t));
                    throw t;
                } else {
                    return EntryMeta.absent;
                }
            } finally {
                p.releaseResult();
            }
        } else {
            // we're already waiting or calculating the entry, await
            return await(key, prevLoad);
        }
    }

    public Object ifCached(Object conn, Segment segment, Object key) {
        try {
            Long fadeMs = fadeMs(segment);
            // no entry in maintenance map, let's try to claim it in Redis
            IPersistentVector entry = s.fetchEntry(conn, key, s.newLoadMarker(), LOAD_MARKER_FADE_SEC * 1000, fadeMs);
            if (entry.valAt(0) != null) {
                Object cachedVal = entry.valAt(1);
                if (!s.isLoadMarker(cachedVal) && !LockoutMap.awaitLockout(cachedVal)) {
                    return EntryMeta.unwrap(cachedVal);
                }
            }
            return EntryMeta.absent;
        } catch (Exception e) {
            return EntryMeta.absent;
        }
    }

    public void invalidate(Object conn, Object k) {
        ConcurrentHashMap<Object, Load> m = connMap(conn);
        Load l = m.remove(k);
        if (l != null) {
            l.getPromise().invalidate();
            l.getPromise().releaseResult();
        }
    }

    public void invalidateByPred(Object conn, IFn pred) {
        ConcurrentHashMap<Object, Load> m = connMap(conn);
        Iterator<Map.Entry<Object, Load>> iter = m.entrySet().iterator();
        List<SpecialPromise> promises = new ArrayList<>();
        while(iter.hasNext()) {
            Map.Entry<Object, Load> e = iter.next();
            if (pred.invoke(e.getKey()) == Boolean.TRUE) {
                SpecialPromise p = e.getValue().getPromise();
                p.invalidate();
                promises.add(p);
            }
        }
        for(SpecialPromise p : promises) {
            p.releaseResult();
        }
    }

    public ConcurrentHashMap<Object, ConcurrentHashMap<Object, Load>> getMaint() {
        return maint;
    }

    public static void addInvalidations(ConcurrentHashMap<Object, ConcurrentHashMap<Object, Load>> maint, Iterable<Object> iterable) {
        for (ConcurrentHashMap<Object, Load> m : maint.values()) {
            for (Load l : m.values()) {
                l.getPromise().addInvalidIds(iterable);
            }
        }
    }
}
