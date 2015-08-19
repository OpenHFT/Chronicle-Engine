package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.Inode;

/**
 * @author Rob Austin.
 */
public class ChronicleDirectoryEntry extends DirectoryEntry {

    public ChronicleDirectoryEntry(final Inode inode, final String name) {
        super(name, inode, ChronicleStat.toStat(inode));
    }

}
