package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.Inode;
import org.jetbrains.annotations.NotNull;

import java.util.IdentityHashMap;
import java.util.Map;

import static net.openhft.chronicle.engine.nfs.ChronicleNfsFileHandleLookup.fh;

/**
 * @author Rob Austin.
 */
public class ChronicleNfsAssetINode extends Inode {

    private static final Map<Asset, ChronicleNfsAssetINode> POOL = new IdentityHashMap<>();

    private ChronicleNfsAssetINode(@NotNull Asset asset) {
        super(fh(asset));
    }

    public static ChronicleNfsAssetINode acquireINode(@NotNull Asset asset) {
        return POOL.computeIfAbsent(asset, k -> new ChronicleNfsAssetINode(asset));
    }

}
