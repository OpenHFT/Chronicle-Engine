package net.openhft.chronicle.engine.nfs;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.*;
import org.jetbrains.annotations.NotNull;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Inode create(Inode parent, Stat.Type type, String path, Subject subject, int mode) throws IOException {

        if (type == Stat.Type.DIRECTORY) {
            final Asset asset = toAsset(parent).acquireAsset(path);
            return ChronicleAssetInode.aquireINode(asset);
        } else if (type == Stat.Type.REGULAR) {
            final MapView view = toAsset(parent).getView(MapView.class);

            if (view == null)
                throw new UnsupportedOperationException("this asset can not be viewed as a MAP");

            if (view.keyType() != String.class || view.valueType() != String.class)
                throw new UnsupportedOperationException("type of map must be string key and value");

            //noinspection unchecked
            view.put(path, "");

            return ChronicleEntryINode.aquireINode(view, path);
        } else
            throw new UnsupportedOperationException("todo");
    }

    @Override
    public FsStat getFsStat() throws IOException {
        return new FsStat(0, 0, 0, 0);
    }

    @Override
    public Inode getRootInode() throws IOException {
        return root;
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        final Asset child = ((ChronicleAssetInode) parent).getAsset().getChild(path);
        return ChronicleAssetInode.aquireINode(child);
    }

    @Override
    public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public List<DirectoryEntry> list(Inode inode) throws IOException {
        final Asset asset = ((ChronicleAssetInode) inode).getAsset();
        final ArrayList<ChronicleDirectoryEntry> result = new ArrayList<>();
        try {
            asset.forEachChild(c -> result.add(new ChronicleDirectoryEntry(c)));
            return (List) result;
        } catch (InvalidSubscriberException e) {
            throw Jvm.rethrow(e);
        }
    }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        final Asset asset = ((ChronicleAssetInode) parent).getAsset().acquireAsset(path);
        return ChronicleAssetInode.aquireINode(asset);
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        final Asset parent = ((ChronicleAssetInode) src).getAsset();
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
        return ((ChronicleAssetInode) inode).getAsset();
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        if (offset > Integer.MAX_VALUE)
            throw new IllegalStateException("offset too large");
        final Asset asset = toAsset(inode);
        if (inode instanceof ChronicleEntryINode) {
            final ChronicleEntryINode n = (ChronicleEntryINode) asset;
            final MapView mapView = n.entrySupplier().mapView;
            final String key = n.entrySupplier().key;
            long len = Math.min(count, key.length() - offset);

            if (len > Integer.MAX_VALUE)
                throw new IllegalStateException("data too large");

            final Object o = mapView.get(key);

            if (o == null)
                return 0;

            final CharSequence charSequence = o.toString().subSequence((int)
                    offset, (int) len);

            for (int i = 0; i < len; i++) {
                data[i] = (byte) charSequence.charAt(i);
            }
            return (int) len;


        } else {

            throw new UnsupportedOperationException();
        }
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        ((ChronicleAssetInode) parent).getAsset().removeChild(path);
    }

    @Override
    public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {

        final Asset asset = toAsset(inode);
        if (inode instanceof ChronicleEntryINode) {
            final ChronicleEntryINode n = (ChronicleEntryINode) asset;
            final MapView mapView = n.entrySupplier().mapView;
            final String key = n.entrySupplier().key;
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
        throw new UnsupportedOperationException("todo");
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
        throw new UnsupportedOperationException("todo");
    }
}
