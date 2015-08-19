package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
public class FileHandleLookup {

    private static final Map<Object, byte[]> pool = new IdentityHashMap<>();
    private static final Map<Long, Object> reversePool = new ConcurrentHashMap<>();

    private static final AtomicLong inc = new AtomicLong();


    static byte[] acquireFileId(@NotNull Asset asset) {
        return acquireFileId0(asset);
    }

    static byte[] acquireFileId(@NotNull EntryProxy entryProxy) {
        return acquireFileId0(entryProxy);
    }


    /**
     * given an object return a unique byte[] for that object
     *
     * @param object the item to lookup
     * @return a unique byte[] for that object
     */
    private static byte[] acquireFileId0(@NotNull Object object) {

        return pool.computeIfAbsent(object, k -> {
            final long l = inc.incrementAndGet();
            reversePool.put(l, object);
            final byte[] bytes = new byte[8];
            final ByteBuffer wrap = ByteBuffer.wrap(bytes);
            wrap.putLong(l);
            return bytes;
        });

    }

    public static <T> T reverseLookup(byte[] fileId) {
        return (T) reversePool.get(toLong(fileId));
    }

    public static long toLong(byte[] fileId) {
        final ByteBuffer wrap = ByteBuffer.wrap(fileId);
        wrap.clear();
        return wrap.getLong();
    }

    public static boolean isDirectory(byte[] fileId) {
        return (toObject(fileId) instanceof Asset);
    }

    public static Object toObject(byte[] fileId) {
        final long i = toLong(fileId);
        return reversePool.get(i);
    }

}
