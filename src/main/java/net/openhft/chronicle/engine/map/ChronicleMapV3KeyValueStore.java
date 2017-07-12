package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.EngineCluster;
import net.openhft.chronicle.engine.fs.EngineHostDetails;
import net.openhft.chronicle.engine.server.internal.MapReplicationHandler;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.Replica;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.api.session.SessionProvider;
import net.openhft.chronicle.network.cluster.ConnectionManager;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public final class ChronicleMapV3KeyValueStore<K, V> implements KeyValueStore<K, V>, Supplier<EngineReplication> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapV3KeyValueStore.class);

    private final ChronicleMap<K, V> delegate;
    @NotNull
    private final Asset asset;
    private final KVSSubscription<K, V> subscriptions;
    private final EventLoop eventLoop;
    private final SessionProvider sessionProvider;
    private final SessionDetails replicationSessionDetails;
    private EngineReplication engineReplicator;

    public ChronicleMapV3KeyValueStore(final RequestContext requestContext,
                                @NotNull final Asset asset, final int replicaId) {
        this.asset = asset;
        if (replicaId > 127) {
            throw new IllegalArgumentException();
        }

        //noinspection unchecked
        subscriptions = asset.acquireView(ObjectSubscription.class, requestContext);
        subscriptions.setKvStore(this);

        @Nullable HostIdentifier hostIdentifier = null;
        try {
            this.engineReplicator = asset.acquireView(EngineReplication.class);


//            @Nullable final EngineReplicationLangBytesConsumer langBytesConsumer = asset.findView
//                    (EngineReplicationLangBytesConsumer.class);

            hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
            assert hostIdentifier != null;

        } catch (AssetNotFoundException anfe) {
//            if (LOG.isDebugEnabled())
//                Jvm.debug().on(getClass(), "replication not enabled ", anfe);
        }


        eventLoop = asset.findOrCreateView(EventLoop.class);
        assert eventLoop != null;
        sessionProvider = asset.findView(SessionProvider.class);
        eventLoop.start();

        replicationSessionDetails = asset.root().findView(SessionDetails.class);

        Byte localIdentifier = hostIdentifier != null ? hostIdentifier.hostId() : null;
        delegate = createReplicatedMap(requestContext, localIdentifier);
        @Nullable Clusters clusters = asset.findView(Clusters.class);

        if (clusters == null) {
            Jvm.warn().on(getClass(), "no clusters found.");
            return;
        }

        final EngineCluster engineCluster = clusters.get(requestContext.cluster());

        if (engineCluster == null) {
            Jvm.warn().on(getClass(), "no cluster found, name=" + requestContext.cluster());
            return;
        }



        ((ChronicleMapV3EngineReplication) engineReplicator).setChronicleMap((ReplicatedChronicleMap<K, V, ?>) delegate);


//        if (LOG.isDebugEnabled())
//            Jvm.debug().on(getClass(), "hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + engineCluster.hostDetails());

        final byte hostId = hostIdentifier.hostId();
        for (@NotNull EngineHostDetails hostDetails : engineCluster.hostDetails()) {
            try {
                // its the identifier with the larger values that will establish the connection
                byte remoteIdentifier = (byte) hostDetails.hostId();

                if (remoteIdentifier == localIdentifier)
                    continue;


                ConnectionManager connectionManager = engineCluster.findConnectionManager(remoteIdentifier);
                if (connectionManager == null) {
                    Jvm.warn().on(getClass(), "connectionManager==null for remoteIdentifier=" + remoteIdentifier);
                    engineCluster.findConnectionManager(remoteIdentifier);
                    continue;
                }

                connectionManager.addListener((nc, isConnected) -> {

                    if (!isConnected)
                        return;

                    if (nc.isAcceptor())
                        return;

                    @NotNull final String csp = requestContext.fullName();

                    final long lastUpdateTime = ((Replica) delegate).remoteNodeCouldBootstrapFrom(remoteIdentifier);

                    WireOutPublisher publisher = nc.wireOutPublisher();

                    LOGGER.info("Map on {} publishing a replication handler to {}",
                            hostId, nc);
                    // TODO mark.price
                    publisher.publish(MapReplicationHandler.newMapReplicationHandler(lastUpdateTime,
                            delegate.keyClass(), delegate.valueClass(), csp, nc.newCid()));
                });


            } catch (Exception e) {
                Jvm.warn().on(getClass(), "hostDetails=" + hostDetails, e);
            }
        }


    }

    @Override
    public boolean put(final K key, final V value) {
        final V existing = delegate.put(key, value);
        final boolean insert = existing == null;

        notifyPut(key, value, existing, insert);

        return insert;
    }

    @Nullable
    @Override
    public V getAndPut(final K key, final V value) {
        final V existing = delegate.get(key);
        delegate.put(key, value);

        notifyPut(key, value, existing, existing == null);

        return existing;
    }

    @Override
    public boolean remove(final K key) {
        final V existing = delegate.remove(key);
        notifyRemove(key, existing);
        return existing != null;
    }

    @Nullable
    @Override
    public V getAndRemove(final K key) {
        final V existing = delegate.remove(key);
        notifyRemove(key, existing);
        return existing;
    }

    @Nullable
    @Override
    public V getUsing(final K key, final Object value) {
        return delegate.getUsing(key, (V) value);
    }

    @Override
    public long longSize() {
        return delegate.longSize();
    }

    @Override
    public void keysFor(final int segment, final SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(delegate.keySet(), kConsumer);
    }

    @Override
    public int segments() {
        return 1;
    }

    @Override
    public void entriesFor(final int segment, final SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        SubscriptionConsumer.notifyEachEvent(delegate.entrySet(),
                e -> kvConsumer.accept(InsertedEvent.of(asset.fullName(), e.getKey(), e.getValue(), false)));
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean containsValue(final V value) {
        return delegate.containsValue(value);
    }

    @Override
    public void accept(final EngineReplication.ReplicationEntry replicationEntry) {
        LOGGER.info("replicationEntry: {}", replicationEntry);
        final BytesStore actualReplicatedEntry = replicationEntry.value();
        ((ReplicatedChronicleMap<K, V, ?>) delegate).readExternalEntry(actualReplicatedEntry.bytesForRead(), replicationEntry.identifier());
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public KeyValueStore<K, V> underlying() {
        throw new UnsupportedOperationException();
    }

    @Override
    public EngineReplication get() {
        LOGGER.info("Returning replication {}", engineReplicator);
        return engineReplicator;
    }

    private void notifyRemove(final K key, final V value) {
        subscriptions.notifyEvent(RemovedEvent.of(asset.name(), key, value, false));
    }

    private void notifyPut(final K key, final V value, final V previous, final boolean insert) {
        final boolean hasValueChanged = hasValueChanged(value, previous);

        if (insert) {
            subscriptions.notifyEvent(InsertedEvent.of(asset.name(), key, value, false));
        } else if(hasValueChanged) {
            // TODO mark.price equality check by map implementation?
            subscriptions.notifyEvent(UpdatedEvent.of(asset.name(), key, previous,
                    value, false, true));
        }
    }

    private static <V> boolean hasValueChanged(final V value, final V previous) {
        return previous == null ? value != null : !previous.equals(value);
    }

    private static <K, V> ChronicleMap<K, V> createReplicatedMap(
            final RequestContext requestContext, final Byte replicaId) {

        final ChronicleMapBuilder<K, V> builder =
                ChronicleMap.of((Class<K>) requestContext.keyType(), (Class<V>) requestContext.valueType()).
                        entries(requestContext.getEntries()).
                        putReturnsNull(requestContext.putReturnsNull() != null ? requestContext.putReturnsNull() : false);
        if (replicaId != null) {
            builder.replication(replicaId);
        }

        if (requestContext.getConstantSizeKeyExample() != null) {
            builder.constantKeySizeBySample((K) requestContext.getConstantSizeKeyExample());
        } else {
            builder.averageKeySize(requestContext.getAverageKeySize());
        }

        if (requestContext.getConstantSizeValueExample() != null) {
            builder.constantValueSizeBySample((V) requestContext.getConstantSizeValueExample());
        } else {
            builder.averageValueSize(requestContext.getAverageValueSize());
        }

        final ChronicleMap<K, V> map = builder.create();
        return map;
    }
}