package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ChronicleMapV3KeyValueStore<K, V> implements KeyValueStore<K, V> {

    ChronicleMapV3KeyValueStore(final RequestContext requestContext, @NotNull final Asset asset) {
    }

    @Override
    public boolean put(final K key, final V value) {
        return false;
    }

    @Nullable
    @Override
    public V getAndPut(final K key, final V value) {
        return null;
    }

    @Override
    public boolean remove(final K key) {
        return false;
    }

    @Nullable
    @Override
    public V getAndRemove(final K key) {
        return null;
    }

    @Nullable
    @Override
    public V getUsing(final K key, final Object value) {
        return null;
    }

    @Override
    public long longSize() {
        return 0;
    }

    @Override
    public void keysFor(final int segment, final SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {

    }

    @Override
    public void entriesFor(final int segment, final SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {

    }

    @Override
    public void clear() {

    }

    @Override
    public boolean containsValue(final V value) {
        return false;
    }

    @Override
    public void accept(final EngineReplication.ReplicationEntry replicationEntry) {

    }

    @Override
    public void close() {

    }

    @Override
    public Asset asset() {
        return null;
    }

    @Nullable
    @Override
    public KeyValueStore<K, V> underlying() {
        return null;
    }
}
