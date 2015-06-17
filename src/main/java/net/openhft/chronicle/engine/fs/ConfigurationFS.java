package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by peter on 12/06/15.
 */
public class ConfigurationFS implements MountPoint {
    public static final String FSTAB = "fstab.yaml";
    public static final String CLUSTERS = "clusters.yaml";
    private final String assetName;
    private final String directory;
    private AssetTree assetTree;

    public ConfigurationFS(String assetName, String directory) {
        this.assetName = assetName;
        this.directory = directory;
    }

    public void install(AssetTree assetTree) {
        RequestContext context = RequestContext.requestContext(assetName);
        Asset asset = assetTree.acquireAsset(context);
        asset.registerView(AuthenticatedKeyValueStore.class, new FilePerKeyValueStore(context.basePath(directory), asset));
        subscribeTo(assetTree);
    }

    public void subscribeTo(AssetTree assetTree) {
        this.assetTree = assetTree;
        Subscriber<MapEvent> eventSub = this::onFile;
        assetTree.registerSubscriber(assetName, MapEvent.class, eventSub);
    }

    public void onFile(MapEvent<String, String> mapEvent) {
        switch (mapEvent.key()) {
            case FSTAB:
                processFstab(mapEvent.value());
                break;
            case CLUSTERS:
                processClusters(mapEvent.value());
                break;
        }
    }

    private void processClusters(String value) {
        Clusters clusters = new Clusters();
        clusters.readMarshallable(TextWire.from(value));
        clusters.install(assetTree);
    }

    private void processFstab(String value) {
        Fstab fstab = new Fstab();
        fstab.readMarshallable(TextWire.from(value));
        fstab.install(assetTree);
    }

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public String spec() {
        return directory;
    }

    @Override
    public String name() {
        return assetName;
    }
}
