package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.Inode;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * @author Rob Austin.
 */
public class ChronicleAssetInode extends Inode {


    private final Asset asset;
    private static final Map<Asset, ChronicleAssetInode> POOL = new IdentityHashMap<>();

    private ChronicleAssetInode(Asset asset) {
        super(new byte[]{});
        this.asset = asset;
    }

    public Asset getAsset() {
        return asset;
    }

    @Override
    public byte[] getFileId() {
        return super.getFileId();
    }

    @Override
    public byte[] toNfsHandle() {
        return super.toNfsHandle();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public boolean isPesudoInode() {
        return super.isPesudoInode();
    }

    @Override
    public int exportIndex() {
        return super.exportIndex();
    }

    @Override
    public int handleVersion() {
        return super.handleVersion();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public static ChronicleAssetInode aquireINode(Asset asset) {
        return POOL.computeIfAbsent(asset, k -> new ChronicleAssetInode(asset));
    }
}
