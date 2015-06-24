package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Created by Rob Austin
 */
public class VanillaReplication implements Replication {

    private final RequestContext requestContext;
    private final Asset asset;
    private final MapView mapView;

    public VanillaReplication(final RequestContext requestContext, final Asset asset, final MapView mapView) {
        this.requestContext = requestContext;
        this.asset = asset;
        this.mapView = mapView;
    }

    @Override
    public void applyReplication(@NotNull final ReplicationEntry replicatedEntry) {
        ((KeyValueStore) mapView.underlying()).accept(replicatedEntry);
    }

    @Override
    public ModificationIterator acquireModificationIterator(final byte id) {
        EngineReplication engineReplication = ((Supplier<EngineReplication>) mapView.underlying()).get();
        return engineReplication.acquireModificationIterator(id);
    }

    @Override
    public long lastModificationTime(final byte id) {
        EngineReplication engineReplication = ((Supplier<EngineReplication>) mapView.underlying()).get();
        return engineReplication.lastModificationTime(id);
    }
}
