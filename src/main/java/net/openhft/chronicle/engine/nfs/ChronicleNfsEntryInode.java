package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.map.MapView;
import org.dcache.nfs.vfs.FileHandle;
import org.dcache.nfs.vfs.Inode;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.engine.nfs.ChronicleNfsFileHandleLookup.acquireFileId;

/**
 * @author Rob Austin.
 */
public class ChronicleNfsEntryInode extends Inode {

    private static final Map<ChronicleNfsEntryProxy, ChronicleNfsEntryInode> POOL = new ConcurrentHashMap<>();

    private ChronicleNfsEntryInode(@NotNull ChronicleNfsEntryProxy entryProxy) {
        super(fh(entryProxy));
    }

    @NotNull
    private static FileHandle fh(@NotNull ChronicleNfsEntryProxy asset) {
        return new FileHandle(0, 0, 0, acquireFileId(asset));
    }

    public static ChronicleNfsEntryInode aquireINode(@NotNull MapView mapView, @NotNull String key) {
        final ChronicleNfsEntryProxy entry = new ChronicleNfsEntryProxy(mapView, key);
        return POOL.computeIfAbsent(entry, k -> new ChronicleNfsEntryInode(entry));
    }

    public static ChronicleNfsEntryInode getINode(@NotNull MapView mapView, @NotNull String key) {
        return POOL.get(new ChronicleNfsEntryProxy(mapView, key));
    }

}
