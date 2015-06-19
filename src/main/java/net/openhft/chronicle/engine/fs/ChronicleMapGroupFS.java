package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 12/06/15.
 */
public class ChronicleMapGroupFS implements Marshallable, MountPoint, LeafViewFactory<KeyValueStore> {
    String spec, name, cluster;
    int averageValueSize;
    Boolean putReturnsNull, removeReturnsNull;
    private long maxEntries;
    private String baseDir;

    @Override
    public String spec() {
        return spec;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(() -> "spec").text(s -> spec = s)
                .read(() -> "name").text(s -> name = s)
                .read(() -> "cluster").text(s -> cluster = s)
                .read(() -> "maxEntries").int64(e -> maxEntries = e)
                .read(() -> "averageValueSize").int32(e -> averageValueSize = e)
                .read(() -> "putReturnsNull").bool(e -> putReturnsNull = e)
                .read(() -> "removeReturnsNull").bool(e -> removeReturnsNull = e);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "spec").text(spec)
                .write(() -> "name").text(name)
                .write(() -> "cluster").text(cluster)
                .write(() -> "maxEntries").int64(maxEntries)
                .write(() -> "averageValueSize").int32(averageValueSize)
                .write(() -> "putReturnsNull").bool(putReturnsNull)
                .write(() -> "removeReturnsNull").bool(removeReturnsNull);
    }

    @Override
    public void install(String baseDir, AssetTree assetTree) {
        this.baseDir = baseDir;
        RequestContext context = RequestContext.requestContext(name).basePath(baseDir + "/" + spec);
        Asset asset = assetTree.acquireAsset(context);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        asset.addLeafRule(KeyValueStore.class, "use Chronicle Map", this);
    }

    @NotNull
    @Override
    public KeyValueStore create(RequestContext context, Asset asset) throws AssetNotFoundException {
        return new ChronicleMapKeyValueStore(context.keyType(), context.valueType(),
                baseDir + "/" + spec, context.name(), cluster,
                putReturnsNull, removeReturnsNull, averageValueSize,
                maxEntries, asset, asset.acquireView(ObjectKVSSubscription.class, context));
    }
}
