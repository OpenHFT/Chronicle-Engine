package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by peter.lawrey on 05/06/2015.
 */
public class RemoteKeyValueStore<K, MV, V> implements KeyValueStore<K, MV, V> {
    public RemoteKeyValueStore(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this(/* extract what you need here */);
    }

    public RemoteKeyValueStore() {
        // the actual implementation goes here

    }

    @Override
    public boolean put(K key, V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V getAndPut(K key, V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean remove(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V getAndRemove(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V get(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V getUsing(K key, MV value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean containsKey(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean isReadOnly() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long size() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int segments() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int segmentFor(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapReplicationEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset asset() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public KeyValueStore<K, MV, V> underlying() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("todo");
    }
}
