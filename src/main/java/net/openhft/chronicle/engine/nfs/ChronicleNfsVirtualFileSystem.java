package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.AssetTreeStats;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.dcache.nfs.v4.xdr.nfs4_prot.*;

/**
 * @author Rob Austin.
 */
public class ChronicleNfsVirtualFileSystem implements VirtualFileSystem {

    public static final int ATTR_TIMEOUT_MS = 10;
    static final Logger LOGGER = LoggerFactory.getLogger(ChronicleNfsVirtualFileSystem.class);
    private final Inode root;
    @NotNull
    private final AssetTree assetTree;
    ChronicleNfsStat lastChronicleNfsStat = null;
    private ChronicleFsStat chronicleFsStat;
    private long chronicleFsStatMS = 0;

    public ChronicleNfsVirtualFileSystem(@NotNull final AssetTree assetTree) {
        this.assetTree = assetTree;
        root = ChronicleNfsAssetINode.acquireINode(assetTree.root());
    }

    @Override
    public int access(Inode inode, int mode) throws IOException {
        int accessmask = 0;

        if ((mode & ACCESS4_READ) != 0) {
            if (canAccess(inode, ACE4_READ_DATA)) {
                accessmask |= ACCESS4_READ;
            }
        }

        if ((mode & ACCESS4_LOOKUP) != 0) {
            if (canAccess(inode, ACE4_EXECUTE)) {
                accessmask |= ACCESS4_LOOKUP;
            }
        }

        if ((mode & ACCESS4_MODIFY) != 0) {
            if (canAccess(inode, ACE4_WRITE_DATA)) {
                accessmask |= ACCESS4_MODIFY;
            }
        }

        if ((mode & ACCESS4_EXECUTE) != 0) {
            if (canAccess(inode, ACE4_EXECUTE)) {
                accessmask |= ACCESS4_EXECUTE;
            }
        }

        if ((mode & ACCESS4_EXTEND) != 0) {
            if (canAccess(inode, ACE4_APPEND_DATA)) {
                accessmask |= ACCESS4_EXTEND;
            }
        }

        if ((mode & ACCESS4_DELETE) != 0) {
            if (canAccess(inode, ACE4_DELETE_CHILD)) {
                accessmask |= ACCESS4_DELETE;
            }
        }

        return accessmask;
    }

    @Override
    public Inode create(@NotNull Inode parent, org.dcache.nfs.vfs.Stat.Type type, @NotNull String path, Subject subject, int mode) throws IOException {

        if (type == org.dcache.nfs.vfs.Stat.Type.DIRECTORY) {
            final Asset asset = toAsset(parent).acquireAsset(path);
            return ChronicleNfsAssetINode.acquireINode(asset);
        } else if (type == org.dcache.nfs.vfs.Stat.Type.REGULAR) {
            final MapView view = toAsset(parent).acquireView(MapView.class);

            if (view.keyType() != String.class || view.valueType() != String.class)
                throw new UnsupportedOperationException("type of map must be string key and value");

            //noinspection unchecked
            view.put(path, "");
            return ChronicleNfsEntryInode.aquireINode(view, path);
        } else
            throw new UnsupportedOperationException("todo type=" + type);
    }

    @NotNull
    @Override
    public FsStat getFsStat() throws IOException {
        long now = System.currentTimeMillis();
        if (now != chronicleFsStatMS) {
            AssetTreeStats usageStats = assetTree.getUsageStats();
            Runtime rt = Runtime.getRuntime();
            long freeSpace = rt.totalMemory();
            chronicleFsStat = new ChronicleFsStat(freeSpace + usageStats.getSizeInBytes(), freeSpace / 1000 + usageStats.getCount(),
                    usageStats.getSizeInBytes(), usageStats.getCount());
            chronicleFsStatMS = now;
        }
        return chronicleFsStat;
    }

    @Override
    public Inode getRootInode() throws IOException {
        return root;
    }

    @Override
    public Inode lookup(@NotNull Inode parent, @NotNull String path) throws IOException {
        final Asset asset = toAsset(parent);
        final Asset child = asset.getChild(path);
        if (child == null) {
            final MapView view = asset.getView(MapView.class);
            if (view == null)
                throw new NoEntException("MapView not found : path=" + path + " does not exist");
            final ChronicleNfsEntryInode iNode = ChronicleNfsEntryInode.getINode(view, path);
            if (iNode == null)
                throw new NoEntException("Inode not found : path=" + path + " does not exist");
            return iNode;

        }
        return ChronicleNfsAssetINode.acquireINode(child);
    }

    @NotNull
    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public List<DirectoryEntry> list(@NotNull Inode inode) throws IOException {
        final List<DirectoryEntry> result = new ArrayList<>();
        try {
            final Asset asset = toAsset(inode);
            asset.forEachChild(c -> result.add(new ChronicleNfsDirectoryEntry(ChronicleNfsAssetINode.acquireINode(c), c.name())));

            final MapView map = asset.getView(MapView.class);
            if (map != null) {
                map.forEach((k, v) -> {

                    final ChronicleNfsEntryInode chronicleEntryInode = ChronicleNfsEntryInode.aquireINode(map, k.toString());
                    final ChronicleNfsDirectoryEntry e = new ChronicleNfsDirectoryEntry(chronicleEntryInode, k.toString());
                    result.add(e);
                });
            }

            return result;
        } catch (InvalidSubscriberException e) {
            throw Jvm.rethrow(e);
        }

    }

    @Override
    public Inode mkdir(@NotNull Inode parent, String path, Subject subject, int mode) throws IOException {
        final Asset asset = toAsset(parent).acquireAsset(path);
        return ChronicleNfsAssetINode.acquireINode(asset);
    }

    @Override
    public boolean move(@NotNull Inode parent, String oldName, Inode dest, String newName) throws
            IOException {
        final Object d = ChronicleNfsFileHandleLookup.reverseLookup(parent.getFileId());
        if (!(d instanceof Asset))
            throw new IOException("Unsupported: parent type=" + d);

        final Asset asset = (Asset) d;
        if (asset.getChild(oldName) != null)
            throw new IOException("Unsupported: can not a directory move");

        final MapView mapView = asset.acquireView(MapView.class);
        final AtomicBoolean wasReplaced = new AtomicBoolean();

        // we use computeIfAbsent as it can be treated atomically in the future
        //noinspection unchecked
        mapView.computeIfAbsent(newName, k -> {
            //noinspection unchecked
            Object oldValue = mapView.getAndRemove(oldName);
            wasReplaced.set(true);
            return oldValue;
        });

        return wasReplaced.get();
    }

    @Override
    public Inode parentOf(@NotNull Inode inode) throws IOException {
        return ChronicleNfsAssetINode.acquireINode(toAsset(inode).parent());
    }

    @NotNull
    private Asset toAsset(@NotNull Inode inode) {
        return ChronicleNfsFileHandleLookup.reverseLookup(inode.getFileId());
    }

    @Override
    public int read(@NotNull Inode inode, byte[] data, long offset, int count) throws IOException {
        if (offset > Integer.MAX_VALUE)
            throw new IllegalStateException("offset too large");
        final Object object = ChronicleNfsFileHandleLookup.reverseLookup(inode.getFileId());
        if (object instanceof ChronicleNfsEntryProxy) {
            final ChronicleNfsEntryProxy entryProxy = (ChronicleNfsEntryProxy) object;

            final CharSequence value = entryProxy.value();
            if (value == null)
                return 0;

            long len = Math.min(count, value.length() - offset);
            for (int i = 0; i < len; i++) {
                data[i] = (byte) value.charAt(Maths.toInt32(offset + i));
            }

            return (int) len;
        } else
            throw new UnsupportedOperationException();

    }

    private boolean canAccess(Inode inode, int mode) {
        return true;
    }

    @NotNull
    @Override
    public String readlink(Inode inode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void remove(@NotNull Inode parent, String path) throws IOException {
        final Asset asset = ChronicleNfsFileHandleLookup.reverseLookup(parent.getFileId());
        if (asset.getChild(path) == null) {
            final MapView view = asset.getView(MapView.class);
            if (view != null)
                view.remove(path);
        } else
            asset.removeChild(path);
    }

    @NotNull
    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public WriteResult write(@NotNull Inode inode, @NotNull byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {

        final Object object = ChronicleNfsFileHandleLookup.reverseLookup(inode.getFileId());
        if (object instanceof ChronicleNfsEntryProxy) {
            final ChronicleNfsEntryProxy entryProxy = (ChronicleNfsEntryProxy) object;
            final MapView mapView = entryProxy.mapView();
            if (CharSequence.class.isAssignableFrom(mapView.valueType())) {
                final String key = entryProxy.key();
                //noinspection unchecked
                mapView.put(key, new String(data));
            } else {
                throw new UnsupportedOperationException("Cannot convert to text");
            }
            return new WriteResult(StabilityLevel.DATA_SYNC, data.length);
        } else {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        LOGGER.info("... commit not supported");
    }

    @NotNull
    @Override
    public org.dcache.nfs.vfs.Stat getattr(@NotNull Inode inode) throws IOException {
        ChronicleNfsStat lastStat = this.lastChronicleNfsStat;
        if (lastStat != null) {
            if (lastStat.getTimeMS() + ATTR_TIMEOUT_MS >= System.currentTimeMillis())
                if (lastStat.getInode().equals(inode))
                    return lastStat;
//                else
//                    System.out.println("inode");
//            else
//                System.out.println("ctms "+ System.currentTimeMillis()%60000);
        }
        ChronicleNfsStat ret = ChronicleNfsStat.toStat(inode);
        lastChronicleNfsStat = ret;
        return ret;
    }

    @Override
    public void setattr(Inode inode, org.dcache.nfs.vfs.Stat stat) throws IOException {
        LOGGER.info("setattr " + inode + " " + stat + " ignored");
    }

    @NotNull
    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        return ChronicleNfsAcl.getAcl();
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        LOGGER.info("setAcl " + inode + " " + Arrays.toString(acl) + " ignored");
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public AclCheckable getAclCheckable() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public NfsIdMapping getIdMapper() {
        return ChronicleNfsIdMapping.EMPTY;
    }
}
