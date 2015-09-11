package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.FileHandle;
import org.dcache.nfs.vfs.Inode;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rob Austin.
 */
class ChronicleNfsFileHandleLookup {

    private static final Map<Object, byte[]> lookup = new IdentityHashMap<>();
    private static final Map<Long, Object> reverseLookup = new ConcurrentHashMap<>();
    private static final AtomicLong inc = new AtomicLong();

    private static byte[] acquireFileId(@NotNull Asset asset) {
        return acquireFileId0(asset);
    }

    static byte[] acquireFileId(@NotNull ChronicleNfsEntryProxy entryProxy) {
        return acquireFileId0(entryProxy);
    }

    @NotNull
    static FileHandle fh(@NotNull Asset asset) {
        return new FileHandle(0, 1, 0, ChronicleNfsFileHandleLookup.acquireFileId(asset));
    }

    /**
     * given an object return a unique byte[] for that object
     *
     * @param object the item to lookup
     * @return a unique byte[] for that object
     */
    private static byte[] acquireFileId0(@NotNull Object object) {

        return lookup.computeIfAbsent(object, k -> {
            final long l = inc.incrementAndGet();
            reverseLookup.put(l, object);
            final byte[] bytes = new byte[8];
            final ByteBuffer wrap = ByteBuffer.wrap(bytes);
            wrap.putLong(l);
            return bytes;
        });

    }

    @NotNull
    public static <T> T reverseLookup(@NotNull final byte[] fileId) {
        //noinspection unchecked
        return (T) reverseLookup.get(toLong(fileId));
    }

    public static long toLong(@NotNull final byte[] fileId) {
        final ByteBuffer wrap = ByteBuffer.wrap(fileId);
        wrap.clear();
        return wrap.getLong();
    }

    public static Object toObject(@NotNull final byte[] fileId) {
        final long i = toLong(fileId);
        return reverseLookup.get(i);
    }

    public static void put(@NotNull final Inode dest, @NotNull final ChronicleNfsEntryProxy entryProxy) {
        final Long key = toLong(dest.getFileId());
        reverseLookup.put(key, entryProxy);
    }
}
