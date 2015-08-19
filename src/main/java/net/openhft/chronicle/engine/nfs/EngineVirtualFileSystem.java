package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.*;
import org.jetbrains.annotations.NotNull;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.dcache.nfs.v4.xdr.nfs4_prot.*;

/**
 * @author Rob Austin.
 */
public class EngineVirtualFileSystem implements VirtualFileSystem {


    public final Inode root;


    public EngineVirtualFileSystem(@NotNull final AssetTree assetTree) {
        root = ChronicleAssetInode.aquireINode(assetTree.root());
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
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {

        if (type == Stat.Type.DIRECTORY) {
            final Asset asset = toAsset(parent).acquireAsset(path);
            return ChronicleAssetInode.aquireINode(asset);
        } else if (type == Stat.Type.REGULAR) {
            final MapView view = toAsset(parent).acquireView(MapView.class);

            if (view == null)
                throw new UnsupportedOperationException("this asset can not be viewed as a MAP");

            if (view.keyType() != String.class || view.valueType() != String.class)
                throw new UnsupportedOperationException("type of map must be string key and value");

            //noinspection unchecked
            view.put(path, "");

            return ChronicleEntryInode.aquireINode(view, path);
        } else
            throw new UnsupportedOperationException("todo");
    }

    @Override
    public FsStat getFsStat() throws IOException {
        return new FsStat(10, 10, 10, 10);
    }

    @Override
    public Inode getRootInode() throws IOException {
        return root;
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        final Asset asset = toAsset(parent);
        final Asset child = asset.getChild(path);
        if (child == null) {
            final MapView view = asset.getView(MapView.class);
            if (view == null)
                throw new NoEntException("MapView not found : path=" + path + " does not exist");
            final ChronicleEntryInode iNode = ChronicleEntryInode.getINode(view, path);
            if (iNode == null)
                throw new NoEntException("Inode not found : path=" + path + " does not exist");
            return iNode;

        }
        return ChronicleAssetInode.aquireINode(child);
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        final ArrayList<ChronicleDirectoryEntry> result = new ArrayList<>();
        try {
            final Asset asset = toAsset(inode);
            asset.forEachChild(c -> result.add(new ChronicleDirectoryEntry(ChronicleAssetInode.aquireINode(c), c.name())));

            final MapView map = asset.getView(MapView.class);
            if (map != null) {
                map.forEach((k, v) -> {

                    final ChronicleEntryInode chronicleEntryInode = ChronicleEntryInode.aquireINode(map, k.toString());
                    final ChronicleDirectoryEntry e = new ChronicleDirectoryEntry(chronicleEntryInode, k.toString());
                    result.add(e);
                });
            }

            return (List) result;
        } catch (InvalidSubscriberException e) {
            throw Jvm.rethrow(e);
        }

    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        final Asset asset = toAsset(parent).acquireAsset(path);
        return ChronicleAssetInode.aquireINode(asset);
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        final Object o = FileHandleLookup.reverseLookup(src.getFileId());
        final Asset parent = (VanillaAsset) o;

        final Asset child = parent.acquireAsset(oldName);

        parent.removeChild(oldName);

        final Asset newAsset = toAsset(dest).acquireAsset(newName);

        // todo move currently will loose the contents of the existing asset it will create a new
        // todo empty asset
        return true;


    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        final Asset parent = toAsset(inode).parent();
        return ChronicleAssetInode.aquireINode(parent);
    }

    private Asset toAsset(Inode inode) {
        return FileHandleLookup.reverseLookup(inode.getFileId());
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        if (offset > Integer.MAX_VALUE)
            throw new IllegalStateException("offset too large");
        final Object object = FileHandleLookup.reverseLookup(inode.getFileId());
        if (object instanceof EntryProxy) {
            final EntryProxy entryProxy = (EntryProxy) object;

            if (count > Integer.MAX_VALUE)
                throw new IllegalStateException("count too large");
            final Object o = entryProxy.value();

            if (o == null)
                return 0;

            final String value = o.toString();
            long len = Math.min(count, value.length() - offset);

            final CharSequence charSequence = value.subSequence((int)
                    offset, (int) len);

            for (int i = 0; i < len; i++) {
                data[i] = (byte) charSequence.charAt(i);
            }
            return (int) len;


        } else {

            throw new UnsupportedOperationException();
        }
    }


    private boolean canAccess(Inode inode, int mode) {
        return true;
    }


    @Override
    public String readlink(Inode inode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {

        final Asset asset = FileHandleLookup.reverseLookup(parent.getFileId());
        if (asset.getChild(path) == null) {
            final MapView view = asset.getView(MapView.class);
            if (view != null)
                view.remove(path);
        } else
            asset.removeChild(path);
    }


    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {

        final Object object = FileHandleLookup.reverseLookup(inode.getFileId());
        if (object instanceof EntryProxy) {
            final EntryProxy entryProxy = (EntryProxy) object;
            final MapView mapView = entryProxy.mapView();
            final String key = entryProxy.key();
            mapView.put(key, new String(data));
            return new WriteResult(StabilityLevel.DATA_SYNC, data.length);
        } else {

            throw new UnsupportedOperationException();
        }

    }


    @Override
    public void commit(Inode inode, long offset, int count) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        return ChronicleStat.toStat(inode);
    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public AclCheckable getAclCheckable() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public NfsIdMapping getIdMapper() {
        return ChronicleNfsIdMapping.EMPTY;
    }
}
