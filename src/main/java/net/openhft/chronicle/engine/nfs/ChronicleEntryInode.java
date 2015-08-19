package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.map.MapView;
import org.dcache.nfs.vfs.FileHandle;
import org.dcache.nfs.vfs.Inode;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Rob Austin.
 */
public class ChronicleEntryInode extends Inode {


    private static final Map<EntryProxy, ChronicleEntryInode> POOL = new ConcurrentHashMap<>();

    private ChronicleEntryInode(EntryProxy entryProxy) {
        super(fh(entryProxy));
    }

    private static FileHandle fh(@NotNull EntryProxy asset) {
        return new FileHandle(0, 0, 0, FileHandleLookup.acquireFileId(asset));
    }


    public static ChronicleEntryInode aquireINode(MapView mapView, String key) {
        final EntryProxy key1 = new EntryProxy(mapView, key);
        return POOL.computeIfAbsent(key1, k -> new ChronicleEntryInode(key1));
    }

    public static ChronicleEntryInode getINode(MapView mapView, String key) {
        return POOL.get(new EntryProxy(mapView, key));
    }

}
