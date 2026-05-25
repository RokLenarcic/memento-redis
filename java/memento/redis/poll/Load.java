package memento.redis.poll;

import memento.caffeine.SpecialPromise;

import java.nio.ByteBuffer;
import java.util.UUID;

public class Load {

    private static final byte TAG_MARKER = 0x03;
    private static final int MARKER_LEN = 17;

    public static UUID newLoadMarker() {
        return UUID.randomUUID();
    }

    public static byte[] loadMarkerBytes(UUID marker) {
        ByteBuffer bb = ByteBuffer.allocate(MARKER_LEN);
        bb.put(TAG_MARKER);
        bb.putLong(marker.getMostSignificantBits());
        bb.putLong(marker.getLeastSignificantBits());
        return bb.array();
    }

    /**
     * True iff {@code bytes} is the 17-byte raw load-marker wire shape
     * {@code [0x03][16B UUID]}.
     */
    public static boolean isLoadMarker(byte[] bytes) {
        return bytes.length == MARKER_LEN && bytes[0] == TAG_MARKER;
    }

    public static UUID loadMarker(byte[] bytes) {
        if (!isLoadMarker(bytes)) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.get();
        return new UUID(bb.getLong(), bb.getLong());
    }

    private final SpecialPromise promise;
    private volatile UUID loadMarker;
    private volatile Long validationEpoch;
    /** Cache name this Load belongs to. Used by cross-JVM joiner revalidation
     *  to locate the cache's tag-epochs Redis key. May be null in test fixtures
     *  that construct Load objects directly. */
    private final Object cacheName;
    /** KeysGenerator used by this cache. Used by cross-JVM joiner revalidation
     *  to build tag-epochs keys. May be null in test fixtures. */
    private final Object keysGenerator;

    public Load(long epoch) {
        this(epoch, null, null);
    }

    public Load(long epoch, Object cacheName, Object keysGenerator) {
        this.promise = new SpecialPromise(epoch);
        this.loadMarker = newLoadMarker();
        this.cacheName = cacheName;
        this.keysGenerator = keysGenerator;
    }

    public SpecialPromise getPromise() {
        return promise;
    }

    public UUID getLoadMarker() {
        return loadMarker;
    }

    public byte[] loadMarkerBytes() {
        if (loadMarker == null) {
            return null;
        } else {
            return loadMarkerBytes(loadMarker);
        }
    }

    public Long getValidationEpoch() {
        return validationEpoch;
    }

    public void setValidationEpoch(Long validationEpoch) {
        this.validationEpoch = validationEpoch;
    }

    public Object getCacheName() {
        return cacheName;
    }

    public Object getKeysGenerator() {
        return keysGenerator;
    }

    public void ourLoad() {
        promise.ownerThread();
    }

    public void foreignLoad() {
        loadMarker = null;
    }

}
