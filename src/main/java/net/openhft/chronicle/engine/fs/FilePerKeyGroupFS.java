package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

/**
 * Created by peter on 12/06/15.
 */
public class FilePerKeyGroupFS implements Marshallable, MountPoint {
    String spec, name;
    Class valueType;
    boolean recurse;

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
                .read(() -> "valueType").typeLiteral(CLASS_ALIASES::forName, c -> valueType = c)
                .read(() -> "recurse").bool(b -> recurse = b);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "spec").text(spec)
                .write(() -> "name").text(name)
                .write(() -> "valueType").typeLiteral(CLASS_ALIASES.nameFor(valueType))
                .write(() -> "recurse").bool(recurse);
    }

    @Override
    public void install(String baseDir, AssetTree assetTree) {
        RequestContext context = RequestContext.requestContext(name).basePath(baseDir + "/" + spec).recurse(this.recurse).keyType(String.class);
        Asset asset = assetTree.acquireAsset(context);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        asset.registerView(KeyValueStore.class, new FilePerKeyValueStore(context, asset));
    }
}
