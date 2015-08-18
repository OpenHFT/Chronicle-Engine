package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.DirectoryEntry;

/**
 * @author Rob Austin.
 */
public class ChronicleDirectoryEntry extends DirectoryEntry {


    public ChronicleDirectoryEntry(Asset asset) {
        super(asset.toString(), ChronicleAssetInode.aquireINode(asset), ChronicleStat.EMPTY);
    }
}
