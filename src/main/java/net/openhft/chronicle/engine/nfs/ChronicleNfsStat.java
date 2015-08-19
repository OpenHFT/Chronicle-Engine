package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rob Austin.
 */
class ChronicleNfsStat {
    private static final AtomicInteger gen = new AtomicInteger();

    /**
     * applies some default stats
     *
     * @param stat the object to apply the stats to
     */
    private static void applyDefaults(@NotNull Stat stat) {
        stat.setDev(1);
        stat.setIno(1);
        stat.setUid(65534);   // 65534 -> nobody
        stat.setGid(65534);   // 65534 -> nobody
        stat.setRdev(0);
        stat.setGeneration(gen.getAndDecrement()); // a hack to always ensure that gen is
        // different,  otherwise the OS will cache the last result ( for example ls -l may stop
        // working if new stuff was added  )
        stat.setATime(System.currentTimeMillis()); // todo set this to a more reasonable time
        stat.setMTime(System.currentTimeMillis()); // todo set this to a more reasonable time
        stat.setCTime(System.currentTimeMillis()); // todo set this to a more reasonable time
        stat.setFileid(1);
        stat.setNlink(1);
    }

    @NotNull
    public static Stat toStat(@NotNull Inode inode) {
        final Stat result = new Stat();
        applyDefaults(result);
        final byte[] fileId = inode.getFileId();
        final long l = ChronicleNfsFileHandleLookup.toLong(fileId);
        assert l < Integer.MAX_VALUE;
        result.setIno((int) l);
        result.setFileid((int) l);
        final Object o = ChronicleNfsFileHandleLookup.toObject(fileId);
        if (o instanceof Asset) {
            result.setSize(0);
            result.setMode(0777 | org.dcache.nfs.vfs.Stat.S_IFDIR);
        } else if (o instanceof ChronicleNfsEntryProxy) {
            result.setSize(((ChronicleNfsEntryProxy) o).valueSize());
            result.setMode(0777 | org.dcache.nfs.vfs.Stat.S_IFREG);
        } else {
            throw new UnsupportedOperationException("class=" + o.getClass());
        }
        return result;
    }

    /**
     *
     private int _dev; // device number - default assert = 1, each map will have its own unique
     number

     private int _ino; // ino - is a unique number for that entry within the device
     private int _mode; // access mode - when you do a ls -l ( you can see the modes as 777 for
     example rwxrwxrwx )

     private int _nlink; // ?
     private int _owner; // file owner of the file, set with 'chown', default nobody -> 65534
     private int _group; // default nobody -> 65534
     private int _rdev;  // remote device  - default = 0
     private long _size; // file size, default value size ( number of bytes to be read )
     private long _fileid; //
     private long _generation; // modification count ?  default = 1

     private long _atime; // access time
     private long _mtime; // modification time
     private long _ctime; // creation time
     */

}
