package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ChronicleMapV3KeyValueStore<K, V> implements KeyValueStore<K, V> {
    private final ReplicatedChronicleMap<K, V, ?> delegate;
    @NotNull
    private final Asset asset;
    private final KVSSubscription<K, V> subscriptions;

    public ChronicleMapV3KeyValueStore(final RequestContext requestContext,
                                @NotNull final Asset asset, final int replicaId) {
        this.asset = asset;
        if (replicaId > 127) {
            throw new IllegalArgumentException();
        }
        delegate = createReplicatedMap(requestContext, (byte) replicaId);
        //noinspection unchecked
        subscriptions = asset.acquireView(ObjectSubscription.class, requestContext);
        subscriptions.setKvStore(this);
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
        throw new UnsupportedOperationException();
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

    private void notifyRemove(final K key, final V value) {
        subscriptions.notifyEvent(RemovedEvent.of(asset.name(), key, value, false));
    }

    private void notifyPut(final K key, final V value, final V previous, final boolean insert) {
        if (insert) {
            subscriptions.notifyEvent(InsertedEvent.of(asset.name(), key, value, false));
        } else {
            // TODO mark.price equality check by map implementation?
            subscriptions.notifyEvent(UpdatedEvent.of(asset.name(), key, previous,
                    value, false, previous != value));
        }
    }

    private static <K, V> ReplicatedChronicleMap<K, V, ?> createReplicatedMap(final RequestContext requestContext, final byte replicaId) {

        final ChronicleMapBuilder<K, V> builder =
                ChronicleMap.of((Class<K>) requestContext.keyType(), (Class<V>) requestContext.valueType()).
                        entries(requestContext.getEntries()).
                        averageValueSize(requestContext.getAverageValueSize()).
                        averageKeySize(requestContext.getAverageKeySize()).
                        putReturnsNull(requestContext.putReturnsNull() != null ? requestContext.putReturnsNull() : false).
                        replication(replicaId);

        final ChronicleMap<K, V> map = builder.create();
        return (ReplicatedChronicleMap<K, V, ?>) map;
    }
}