package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.ClassLocal;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.api.map.StringMarshallableKeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine2.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class VanillaStringMarshallableKeyValueStore<V extends Marshallable> implements StringMarshallableKeyValueStore<V> {
    private static final ClassLocal<Constructor> CONSTRUCTORS = ClassLocal.withInitial(c -> {
        try {
            Constructor con = c.getDeclaredConstructor();
            con.setAccessible(true);
            return con;
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    });
    private final BiFunction<V, Bytes, Bytes> valueToBytes;
    private final BiFunction<BytesStore, V, V> bytesToValue;
    private final SubscriptionKVSCollection<String, V, V> subscriptions = new SubscriptionKVSCollection<>(this);
    private SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore;
    private Asset asset;

    public VanillaStringMarshallableKeyValueStore(RequestContext<SubscriptionKeyValueStore<String, Bytes, BytesStore>> context) {
        this(context.parent(), context.type2(), context.item(), context.wireType());
    }

    VanillaStringMarshallableKeyValueStore(Asset asset, Class type2, SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore, Function<Bytes, Wire> wireType) {
        this.asset = asset;
        valueToBytes = toBytes(type2, wireType);
        bytesToValue = fromBytes(type2, wireType);
        this.kvStore = kvStore;
    }

    static <T> BiFunction<T, Bytes, Bytes> toBytes(Class type, Function<Bytes, Wire> wireType) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.append((String) t);
        if (Marshallable.class.isAssignableFrom(type))
            return (t, bytes) -> {
                t = acquireInstance(type, t);
                ((Marshallable) t).writeMarshallable(wireType.apply(bytes));
                bytes.flip();
                return bytes;
            };
        throw new UnsupportedOperationException("todo");
    }

    static <T> T acquireInstance(Class type, T t) {
        if (t == null)
            try {
                t = (T) CONSTRUCTORS.get(type).newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        return t;
    }

    private <T> BiFunction<BytesStore, T, T> fromBytes(Class type, Function<Bytes, Wire> wireType) {
        if (type == String.class)
            return (t, bytes) -> (T) (bytes == null ? null : bytes.toString());
        if (Marshallable.class.isAssignableFrom(type))
            return (bytes, t) -> {
                t = acquireInstance(type, t);
                ((Marshallable) t).readMarshallable(wireType.apply(bytes.bytes()));
                ((Bytes) bytes).position(0);
                return t;
            };
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V getAndPut(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        BytesStore retBytes = kvStore.getAndPut(key, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public V getAndRemove(String key) {
        Buffers b = BUFFERS.get();
        BytesStore retBytes = kvStore.getAndRemove(key);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public V getUsing(String key, V value) {
        Buffers b = BUFFERS.get();
        BytesStore retBytes = kvStore.getUsing(key, b.valueBuffer);
        return retBytes == null ? null : bytesToValue.apply(retBytes, value);
    }

    @Override
    public long size() {
        return kvStore.size();
    }

    @Override
    public void keysFor(int segment, Consumer<String> kConsumer) {
        kvStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<String, V>> kvConsumer) {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(
                Entry.of(e.key(),
                        bytesToValue.apply(e.value(), null))));
    }

    @Override
    public Iterator<Map.Entry<String, V>> entrySetIterator() {
        List<Map.Entry<String, V>> entries = new ArrayList<>();
        for (int i = 0, seg = segments(); i < seg; i++)
            entriesFor(i, e -> entries.add(new AbstractMap.SimpleEntry<>(e.key(), e.value())));
        return entries.iterator();
    }

    @Override
    public void clear() {
        kvStore.clear();
    }

    @Override
    public void asset(Asset asset) {
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore<String, V, V> underlying) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public KeyValueStore underlying() {
        return kvStore;
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        Class eClass = rc.type();
        if (eClass == MapEvent.class) {
            Subscriber<MapEvent<String, V>> sub = (Subscriber<MapEvent<String, V>>) subscriber;

            kvStore.registerSubscriber(rc, (MapEvent<String, BytesStore> e) -> {
                if (e.getClass() == InsertedEvent.class)
                    sub.on(InsertedEvent.of(e.key(), bytesToValue.apply(e.value(), null)));
                else if (e.getClass() == UpdatedEvent.class)
                    sub.on(UpdatedEvent.of(e.key(), bytesToValue.apply(((UpdatedEvent<String, BytesStore>) e).oldValue(), null), bytesToValue.apply(e.value(), null)));
                else
                    sub.on(RemovedEvent.of(e.key(), bytesToValue.apply(e.value(), null)));
            });
        } else {
            subscriptions.registerSubscriber(rc, subscriber);
        }
    }

    @Override
    public <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        kvStore.registerTopicSubscriber(tClass, eClass, (topic, message) -> {
            throw new UnsupportedOperationException("todo");
        }, query);
        subscriptions.registerTopicSubscriber(tClass, eClass, subscriber, query);
    }

    @Override
    public <E> void unregisterSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <T, E> void unregisterTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException();
    }
}
