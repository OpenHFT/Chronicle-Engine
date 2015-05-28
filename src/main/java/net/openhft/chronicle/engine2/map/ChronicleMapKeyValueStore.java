package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;
import net.openhft.chronicle.hash.Value;
import net.openhft.chronicle.map.*;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by daniel on 27/05/15.
 */
public class ChronicleMapKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V>, Closeable {
    private final ChronicleMap<K,V> chronicleMap;
    private final SubscriptionKVSCollection<K, MV, V> subscriptions = new SubscriptionKVSCollection<>(this);
    private Asset asset;

    public ChronicleMapKeyValueStore(RequestContext context){
        PublishingOperations publishingOperations = new PublishingOperations();

        Class kClass = context.type();
        Class vClass = context.type2();

        String basePath = context.basePath();

        ChronicleMapBuilder builder = ChronicleMapBuilder.of(kClass, vClass)
                .entryOperations(publishingOperations);

        if(basePath!=null) {
            try {
                builder.createPersistedTo(new File(basePath));
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }

        chronicleMap = builder.create();
    }

    @Override
    public V getAndPut(K key, V value) {
        return chronicleMap.put(key, value);
    }

    @Override
    public V getAndRemove(K key) {
        return chronicleMap.remove(key);
    }

    @Override
    public V getUsing(K key, MV value) {
        //TODO is this cast correct?
        return chronicleMap.getUsing(key, (V)value);
    }

    @Override
    public long size() {
        return chronicleMap.size();
    }

    @Override
    public void keysFor(int segment, Consumer<K> kConsumer) {
        //TODO can't see a way of implementing this on ChronicleMap
        throw new UnsupportedOperationException();
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<K, V>> kvConsumer) {
        //TODO can't see a way of implementing this on ChronicleMap
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return chronicleMap.entrySet().iterator();
    }

    @Override
    public void clear() {
        chronicleMap.clear();
    }

    @Override
    public void asset(Asset asset) {
        if (this.asset != null) throw new IllegalStateException();
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore<K, MV, V> underlying) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueStore<K, MV, V> underlying() {
        return null;
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        subscriptions.registerSubscriber(rc, subscriber);
    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
        subscriptions.registerTopicSubscriber(rc, subscriber);
    }

    @Override
    public void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {
        subscriptions.unregisterSubscriber(rc, subscriber);
    }

    @Override
    public void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        subscriptions.unregisterTopicSubscriber(rc, subscriber);
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        return this;
    }

    @Override
    public void close() throws IOException {
        chronicleMap.close();
    }

    class PublishingOperations implements MapEntryOperations<K, V, Void> {
        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            Void v = MapEntryOperations.super.remove(entry);
            subscriptions.notifyRemoval(entry.key().get(), entry.value().get());
            return v;
        }
        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Value<V, ?> newValue) {
            V oValue = entry.value().get();
            V nValue = newValue.get();
            Void v = MapEntryOperations.super.replaceValue(entry, newValue);
            subscriptions.notifyUpdate(entry.key().get(), oValue, nValue);
            return v;
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Value<V, ?> value) {
            Void v = MapEntryOperations.super.insert(absentEntry, value);
            subscriptions.notifyUpdate(absentEntry.absentKey().get(), null, value.get());
            return v;
        }
    }
}
