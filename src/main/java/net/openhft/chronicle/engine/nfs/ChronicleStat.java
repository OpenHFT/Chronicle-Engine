package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.tree.Asset;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Rob Austin.
 */
public class ChronicleStat extends Stat {
    public static AtomicInteger gen = new AtomicInteger();
    public static final ChronicleStat EMPTY = new ChronicleStat();

    static {
        applyDefault(EMPTY);
    }

    private static void applyDefault(ChronicleStat stat) {
        stat.setDev(1);
        stat.setIno(1);
        stat.setMode(0755 | Stat.S_IFDIR);   // rwxrwxrwx
        stat.setUid(65534);   // 65534 -> nobody
        stat.setGid(65534);   // 65534 -> nobody
        stat.setRdev(0);
        stat.setSize(100);
        stat.setGeneration(gen.getAndDecrement()); // a hack to always ensure that gen is
        // different,  otherwise the OS will cache the last result ( for example ls -l may stop
        // working if new stuff was added  )
        stat.setATime(System.currentTimeMillis());
        stat.setMTime(System.currentTimeMillis());
        stat.setCTime(System.currentTimeMillis());
        stat.setFileid(1);
        stat.setNlink(1);
    }

    public static Stat toStat(@NotNull Inode inode) {
        final ChronicleStat result = new ChronicleStat();
        applyDefault(result);
        final byte[] fileId = inode.getFileId();
        final long l = FileHandleLookup.toLong(fileId);
        assert l < Integer.MAX_VALUE;
        result.setIno((int) l);
        result.setFileid((int) l);
        final Object o = FileHandleLookup.toObject(fileId);
        if (o instanceof Asset)
            result.setMode(0777 | Stat.S_IFDIR);
        else if (o instanceof EntryProxy) {
            result.setSize(((EntryProxy) o).valueSize());
            result.setMode(0777 | Stat.S_IFREG);
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
