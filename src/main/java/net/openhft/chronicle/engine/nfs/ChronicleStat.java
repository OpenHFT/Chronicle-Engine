package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.vfs.Stat;

/**
 * @author Rob Austin.
 */
public class ChronicleStat extends Stat {

    public static final ChronicleStat EMPTY = new ChronicleStat();

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
