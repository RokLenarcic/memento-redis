package memento.redis;

import clojure.lang.IFn;
import clojure.lang.IPersistentSet;
import clojure.lang.RT;
import clojure.lang.Symbol;
import memento.base.EntryMeta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class EntryEnvelope {

    // -------------------------------------------------------------------------
    // Wire protocol (see doc/future-tag-invalidation.md)
    //
    // All Redis values are framed with a leading discriminator byte:
    //   0x01  untagged entry:  [0x01][nippy(value)]
    //   0x02  tagged entry:    [0x02][nippy(tagIdents)][nippy(value)]
    //   0x03  load marker:     [0x03][16B UUID] (handled by Load)
    // -------------------------------------------------------------------------

    public static final byte TAG_UNTAGGED = 0x01;
    public static final byte TAG_TAGGED = 0x02;
    public static final byte TAG_LOAD_MARKER = 0x03;

    private static final IFn FREEZE_TO_OUT;
    private static final IFn THAW_FROM_IN;

    static {
        RT.var("clojure.core", "require").invoke(Symbol.intern("taoensso.nippy"));
        FREEZE_TO_OUT = RT.var("taoensso.nippy", "freeze-to-out!");
        THAW_FROM_IN = RT.var("taoensso.nippy", "thaw-from-in!");
    }

    private EntryEnvelope() {
    }

    public static byte[] writeEnvelope(Object value) {
        Object entryValue = value;
        IPersistentSet tagIdents = null;
        if (value instanceof EntryMeta) {
            EntryMeta entry = (EntryMeta) value;
            if (entry.isNoCache()) {
                throw new IllegalArgumentException("Cannot store a no-cache EntryMeta");
            }
            entryValue = entry.getV();
            tagIdents = entry.getTagIdents();
        }
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
            DataOutputStream dos = new DataOutputStream(baos);
            if (tagIdents == null || tagIdents.count() == 0) {
                dos.writeByte(TAG_UNTAGGED);
                FREEZE_TO_OUT.invoke(dos, entryValue);
            } else {
                dos.writeByte(TAG_TAGGED);
                FREEZE_TO_OUT.invoke(dos, tagIdents);
                FREEZE_TO_OUT.invoke(dos, entryValue);
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns true if {@code value} is a non-empty {@code byte[]} whose first
     * byte is a known value-envelope discriminator (0x01 untagged or 0x02
     * tagged). Returns false for load markers, other byte sequences, and
     * non-byte[] values. Callers that need to distinguish entries from load
     * markers or corrupt bytes should test with this before decoding.
     */
    public static boolean isEntryEnvelope(Object value) {
        if (!(value instanceof byte[])) {
            return false;
        }
        byte[] env = (byte[]) value;
        if (env.length == 0) {
            return false;
        }
        byte tag = env[0];
        return tag == TAG_UNTAGGED || tag == TAG_TAGGED;
    }

    /**
     * Decode an envelope byte array. Pure: no side effects, no callbacks.
     *   0x01 untagged → value (cached nil is wrapped as EntryMeta)
     *   0x02 tagged   → EntryMeta carrying tag idents
     *   0x03 load marker → EntryMeta.absent
     *   anything else → EntryMeta.absent
     *
     * Callers that wish to delete corrupt or unexpected bytes from Redis
     * must do so explicitly; this method never mutates Redis state.
     */
    public static Object readEnvelope(byte[] env) {
        ByteArrayInputStream bais = new ByteArrayInputStream(env, 1, env.length - 1);
        DataInputStream dis = new DataInputStream(bais);
        switch (env[0]) {
            case TAG_UNTAGGED:
                Object value = THAW_FROM_IN.invoke(dis);
                return value == null ? new EntryMeta(null, false, null) : value;
            case TAG_TAGGED:
                IPersistentSet tagIdents = (IPersistentSet) THAW_FROM_IN.invoke(dis);
                return new EntryMeta(THAW_FROM_IN.invoke(dis), false, tagIdents);
            default:
                return EntryMeta.absent;
        }
    }

    /**
     * Convenience for {@code asMap}-style readers: if {@code value} is a known
     * entry envelope (0x01 or 0x02), decode it; otherwise return
     * {@link EntryMeta#absent} so the caller can elide the pair.
     */
    public static Object readEnvelopeIfEntry(Object value) {
        if (!isEntryEnvelope(value)) {
            return EntryMeta.absent;
        }
        return readEnvelope((byte[]) value);
    }
}
