package memento.redis.poll;

import clojure.lang.*;
import memento.base.Durations;
import memento.base.EntryMeta;
import memento.base.InvalidationClock;
import memento.base.Segment;
import memento.base.TagInvalidation;
import memento.caffeine.SpecialPromise;
import memento.redis.EntryEnvelope;

import java.util.ArrayList;
import java.util.HashMap;
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


    public void putEntries(Object conn, Segment segment, IPersistentMap argsToVals, IFn keyFn) {
        HashMap<Long, ArrayList<EntryPut>> untaggedByExpiry = new HashMap<>();
        for (ISeq seq = argsToVals.seq(); seq != null; seq = seq.next()) {
            IMapEntry e = (IMapEntry) seq.first();
            Object key = keyFn.invoke(segment, e.key());
            Object value = e.val();
            IPersistentSet tagIdents = tagIdents(value);
            Long expire = expiryMs(segment, key, value);
            if (tagIdents != null && tagIdents.count() > 0) {
                s.putValue(conn, cacheName, keysGenerator, key, value, expire);
            } else {
                untaggedByExpiry.computeIfAbsent(expire, x -> new ArrayList<>()).add(new EntryPut(key, value));
            }
        }
        for (Map.Entry<Long, ArrayList<EntryPut>> e : untaggedByExpiry.entrySet()) {
            ArrayList<EntryPut> puts = e.getValue();
            for (int offset = 0; offset < puts.size(); offset += 100) {
                int end = Math.min(offset + 100, puts.size());
                ArrayList<Object> keys = new ArrayList<>(end - offset);
                ArrayList<Object> values = new ArrayList<>(end - offset);
                for (int i = offset; i < end; i++) {
                    EntryPut put = puts.get(i);
                    keys.add(put.key);
                    values.add(put.value);
                }
                s.putValues(conn, keys, values, e.getKey());
            }
        }
    }

    private static IPersistentSet tagIdents(Object value) {
        if (value instanceof EntryMeta) {
            EntryMeta entry = (EntryMeta) value;
            if (entry.isNoCache()) {
                throw new IllegalArgumentException("Cannot store a no-cache EntryMeta");
            }
            return entry.getTagIdents();
        }
        return null;
    }

    private static final class EntryPut {
        private final Object key;
        private final Object value;

        private EntryPut(Object key, Object value) {
            this.key = key;
            this.value = value;
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

    private IObj markCachedMeta(IObj o) {
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
        // §5.9: joiners (same-JVM or cross-JVM) must observe the same
        // hit-marking behavior as direct hits served by the get hit branch.
        // markCached is a no-op when doHitDetection is false.
        Object v = EntryMeta.unwrap(l.getPromise().await(key));
        return markCached(v);
    }

    /**
     * Resolve a value for {@code key}, either by loading it ourselves or by joining an
     * in-progress load. Returns {@link EntryMeta#absent} to signal the caller to re-loop
     * (the load was invalidated mid-flight or raced with another writer).
     * <p>
     * Orchestration of the our-load branch:
     * <ol>
     *   <li>{@code loads.putIfAbsent} arbitrates which thread becomes the loader for this
     *       key. Concurrent callers in the same JVM observe the existing {@link Load} and
     *       go down the joiner branch ({@code await} on its {@link SpecialPromise}).</li>
     *   <li>{@code fetchEntry} (fetch.lua) atomically either claims the Redis slot with our
     *       load marker (and captures the cache's current {@code validation_epoch}) or
     *       reports an existing value / foreign load marker.</li>
     *   <li>If we won the claim we invoke the user fn and call {@code p.deliver} to publish
     *       the value to joiners already blocked on {@code SpecialPromise.await} — that is
     *       the sole channel they receive it through; they will <em>not</em> re-read the
     *       delegate map.</li>
     *   <li>{@code completeLoad} writes to Redis. The tagged variant (finish-load-w-sec.lua)
     *       can return {@code -1} if a tag we depend on was invalidated between fetch and
     *       finish. On {@code -1} we {@code p.reject()} — this overwrites the promise's
     *       result with {@link EntryMeta#absent} so joiners woken by {@code releaseResult}
     *       observe a miss and re-loop. The script itself removes our load marker from Redis
     *       on the {@code -1} path, so cross-process joiners also retry promptly.</li>
     *   <li>The {@code finally} performs cleanup in two steps, in order:
     *     <ul>
     *       <li>{@code loads.remove(key, newLoad)} — identity-safe (only removes if the
     *           mapping is still {@code newLoad}). Running this <em>after</em> {@code deliver}
     *           maximises same-JVM coalescing: late same-JVM callers that arrive between
     *           {@code deliver} and this removal still hit the joiner branch and attach to
     *           our promise, immediately reading the published result once the latch opens.
     *           Redis arbitrates any duplicate work across processes via the load-marker
     *           check in finish-load.lua, so this strictly improves coalescing without
     *           weakening cross-JVM correctness.</li>
     *       <li>{@code p.releaseResult()} opens the latch.
     *           <strong>It must run after {@code deliver}/{@code reject}/{@code deliverException}</strong>
     *           so joiners observe a terminal state, never a transient one. Java's try/finally
     *           guarantees this ordering.</li>
     *     </ul>
     *     This (remove, then release) ordering is applied consistently across the cached-hit,
     *     local-load, and {@code em.isNoCache()} paths.
     *   </li>
     * </ol>
     * The {@code em.isNoCache()} sub-branch is a special case: the value is delivered through
     * the promise channel (joiners get it once) and {@code abandonLoad} clears the load marker
     * in Redis, but no value is written. The cache stays empty for this key.
     */
    public Object get(Object conn, Segment segment, ISeq args, Object key) throws Throwable {
        Load newLoad = new Load(InvalidationClock.current(), cacheName, keysGenerator);
        ConcurrentHashMap<Object, Load> loads = connMap(conn);
        Load prevLoad = loads.putIfAbsent(key, newLoad);
        if (prevLoad == null) {
            SpecialPromise p = newLoad.getPromise();
            IPersistentVector entry = fetchEntry(conn, segment, args, key, newLoad, p, loads);
            if (entry == null) {
                return EntryMeta.absent;
            }

            // car/parse-raw decodes Lua true as 1 and false as nil.
            if (entry.valAt(0) != null) {
                byte[] raw = (byte[])entry.valAt(1);
                if (Load.isLoadMarker(raw)) {
                    // Foreign loader owns the slot. We do not own a Redis marker;
                    // maintenance or invalidation is responsible for completing this promise.
                    newLoad.foreignLoad();
                    return await(key, newLoad);
                }

                try {
                    // Unknown / corrupt bytes (e.g. version 1.x entries): drop
                    // the key so we don't keep returning absent forever
                    // (§5.4.2 livelock prevention). Marker bytes were already
                    // handled above, so this only fires on stale/legacy values.
                    if (!EntryEnvelope.isEntryEnvelope(raw)) {
                        s.delEntry(conn, key);
                        return EntryMeta.absent;
                    }
                    // value envelope - dispatch on discriminator (§5.4)
                    Object ret = EntryEnvelope.readEnvelope(raw);
                    Object marked = markCached(ret);
                    long invEpoch = latestTagInvalidation(ret);
                    return p.deliver(marked, invEpoch) ? EntryMeta.unwrap(marked) : EntryMeta.absent;
                } catch (Throwable t) {
                    if (!p.isInvalid()) {
                        p.deliverException(retExFn == null ? t : (Throwable) retExFn.invoke(args, t));
                        throw t;
                    }
                    return EntryMeta.absent;
                } finally {
                    // See class-level Javadoc on get(): identity-safe remove-then-release,
                    // run after deliver so late same-JVM joiners coalesce onto our promise.
                    loads.remove(key, newLoad);
                    p.releaseResult();
                }
            } else {
                try {
                    Long validationEpoch = ((Number)entry.valAt(2)).longValue();
                    newLoad.setValidationEpoch(validationEpoch);
                    newLoad.ourLoad();
                    Object result = AFn.applyToHelper(segment.getF(), args);
                    if (retFn != null) {
                        result = retFn.invoke(args, result);
                    }
                    if (!p.deliver(result, latestTagInvalidation(result))) {
                        // The SpecialPromise was invalidated
                        s.abandonLoad(conn, key, newLoad);
                        return EntryMeta.absent;
                    }
                    Long writeExpiry = expiryMs(segment, key, result);
                    boolean isNoCache = false;
                    IPersistentSet tagIdents = null;
                    if (result instanceof EntryMeta) {
                        EntryMeta em = (EntryMeta) result;
                        isNoCache = em.isNoCache();
                        result = em.getV();
                        tagIdents = em.getTagIdents();
                    }
                    if (isNoCache) {
                        s.abandonLoad(conn, key, newLoad);
                        return result;
                    } else if (s.completeLoad(conn, completeLoadKeys(key, tagIdents), result, newLoad, writeExpiry) == LoaderSupport.COMPLETE_LOAD_STALE_TAG) {
                        p.reject();
                        return EntryMeta.absent;
                    } else {
                        return result;
                    }
                } catch (Throwable t) {
                    try {
                        s.abandonLoad(conn, key, newLoad);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (!p.isInvalid()) {
                        p.deliverException(retExFn == null ? t : (Throwable) retExFn.invoke(args, t));
                        throw t;
                    }
                    return EntryMeta.absent;
                } finally {
                    loads.remove(key, newLoad);
                    p.releaseResult();
                }
            }
        } else {
            // we're already waiting or calculating the entry, await
            return await(key, prevLoad);
        }
    }

    private IPersistentVector fetchEntry(Object conn, Segment segment, ISeq args, Object key, Load newLoad, SpecialPromise p, ConcurrentHashMap<Object, Load> loads) throws Throwable {
        try {
            Long fadeMs = fadeMs(segment);
            // no entry in maintenance map, let's try to claim it in Redis
            return s.fetchEntry(conn, key, newLoad, LOAD_MARKER_FADE_SEC * 1000, fadeMs);
        } catch (Throwable t) {
            loads.remove(key, newLoad);
            try {
                s.abandonLoad(conn, key, newLoad);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (p.isInvalid()) {
                p.releaseResult();
                return null;
            } else {
                p.deliverException(retExFn == null ? t : (Throwable) retExFn.invoke(args, t));
                p.releaseResult();
                throw t;
            }
        }
    }

    public Object ifCached(Object conn, Segment segment, Object key) {
        try {
            Load localLoad = connMap(conn).get(key);
            if (localLoad != null) {
                return EntryMeta.unwrap(localLoad.getPromise().getNow());
            }
            Long fadeMs = fadeMs(segment);
            IPersistentVector entry = s.fetchCachedEntry(conn, cacheName, keysGenerator, key, fadeMs);
            // car/parse-raw decodes Lua true as 1 and false as nil.
            if (entry.valAt(0) != null) {
                byte[] raw = (byte[])entry.valAt(1);
                if (Load.isLoadMarker(raw)) {
                    // A foreign loader currently owns this key. Do not delete
                    // the marker; just report absent so the caller treats this
                    // as "no completed entry available right now".
                    return EntryMeta.absent;
                }
                if (EntryEnvelope.isEntryEnvelope(raw)) {
                    return EntryMeta.unwrap(EntryEnvelope.readEnvelope(raw));
                }
                // Unknown / corrupt bytes: drop the key so we don't keep
                // returning absent forever (§5.4.2 livelock prevention).
                s.delEntry(conn, key);
                return EntryMeta.absent;
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
                iter.remove();
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
        ArrayList<Object> invalidations = new ArrayList<>();
        for (Object id : iterable) {
            invalidations.add(id);
        }
        for (ConcurrentHashMap<Object, Load> m : maint.values()) {
            for (Load l : m.values()) {
                l.getPromise().addInvalidIds(invalidations);
            }
        }
    }

    private long latestTagInvalidation(Object value) {
        if (value instanceof EntryMeta) {
            return TagInvalidation.INSTANCE.lastInvalidatedEpoch(((EntryMeta) value).getTagIdents());
        }
        return InvalidationClock.NO_INVALIDATION_EPOCH;
    }

}
