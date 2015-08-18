package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.engine.api.map.MapView;
import org.dcache.nfs.vfs.Inode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Rob Austin.
 */
public class ChronicleEntryInode extends Inode {

    private static final Map<EntrySupplier, ChronicleEntryInode> POOL = new ConcurrentHashMap<>();
    private EntrySupplier entrySupplier = null;

    private ChronicleEntryInode(EntrySupplier entrySupplier) {
        super(new byte[]{});
        this.entrySupplier = entrySupplier;
    }

    public static ChronicleEntryInode aquireINode(MapView mapView, String key) {
        final EntrySupplier key1 = new EntrySupplier(mapView, key);
        return POOL.computeIfAbsent(key1, k -> new ChronicleEntryInode(key1));
    }

    public EntrySupplier entrySupplier() {
        return this.entrySupplier;

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

    /**
     * holds a reference to the map and the key of interest the reason that we don hold a reference
     * to the entry is that chronicle map stores its entries off heap
     */
    static class EntrySupplier {
        MapView mapView;
        String key;

        public EntrySupplier(MapView mapView, String key) {
            this.mapView = mapView;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof EntrySupplier)) return false;

            EntrySupplier that = (EntrySupplier) o;

            if (mapView != null ? !mapView.equals(that.mapView) : that.mapView != null)
                return false;
            return !(key != null ? !key.equals(that.key) : that.key != null);

        }

        @Override
        public int hashCode() {
            int result = mapView != null ? mapView.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }
    }
}
