package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.vfs.Stat;

/**
 * @author Rob Austin.
 */
public class ChronicleStat extends Stat {

    public static final ChronicleStat EMPTY = new ChronicleStat();
}
