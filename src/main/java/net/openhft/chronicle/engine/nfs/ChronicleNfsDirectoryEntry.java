package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.Inode;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.engine.nfs.ChronicleNfsStat.toStat;

/**
 * @author Rob Austin.
 */
class ChronicleNfsDirectoryEntry extends DirectoryEntry {

    public ChronicleNfsDirectoryEntry(@NotNull final Inode inode, @NotNull final String name) {
        super(name, inode, toStat(inode));
    }

}
