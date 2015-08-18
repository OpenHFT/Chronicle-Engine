package net.openhft.chronicle.engine.nfs;

import org.dcache.nfs.vfs.FileHandle;

/**
 * @author Rob Austin.
 */
public class ChronicleFileHandle extends FileHandle {

    private byte[] bytes;


    public ChronicleFileHandle(byte[] bytes) {
        super(bytes);
        this.bytes = bytes;
    }


    public ChronicleFileHandle(int generation, int exportIdx, int type, byte[] fs_opaque) {
        super(new byte[]{});
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int getVersion() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int getMagic() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int getGeneration() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int getExportIdx() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int getType() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public byte[] getFsOpaque() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public byte[] bytes() {
        return super.bytes();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
