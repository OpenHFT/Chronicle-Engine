package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.FileHandle;
import org.dcache.nfs.vfs.Inode;
import org.jetbrains.annotations.NotNull;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class ChronicleAssetInode extends Inode {

    private static final Map<Asset, ChronicleAssetInode> POOL = new IdentityHashMap<>();

    private ChronicleAssetInode(@NotNull Asset asset) {
        super(fh(asset));
    }

    public static ChronicleAssetInode aquireINode(@NotNull Asset asset) {
        return POOL.computeIfAbsent(asset, k -> new ChronicleAssetInode(asset));
    }

    private static FileHandle fh(@NotNull Asset asset) {
        return new FileHandle(0, 1, 0, FileHandleLookup.acquireFileId(asset));
    }


}
