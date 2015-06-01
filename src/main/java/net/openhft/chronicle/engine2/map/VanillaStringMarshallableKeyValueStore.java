package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.ClassLocal;
import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.api.map.StringMarshallableKeyValueStore;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
    private SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore;
    private final SubscriptionKVSCollection<String, V> subscriptions = new TranslatingSubscriptionKVSCollection();
    private Asset asset;
    private final Class type2;
    private final Function<Bytes, Wire> wireType;

    public VanillaStringMarshallableKeyValueStore(RequestContext context, Asset asset, Supplier<Assetted> kvStore) {
        this(asset, context.type2(), (SubscriptionKeyValueStore<String, Bytes, BytesStore>) kvStore.get(), context.wireType());
    }

    VanillaStringMarshallableKeyValueStore(Asset asset, Class type2, SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore,
                                           Function<Bytes, Wire> wireType) {
        this.asset = asset;
        this.type2 = type2;
        this.wireType = wireType;
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
    public SubscriptionKVSCollection<String, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public void put(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        kvStore.put(key, valueBytes);
    }

    @Override
    public V getAndPut(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        BytesStore retBytes = kvStore.getAndPut(key, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public void remove(String key) {
        kvStore.remove(key);
    }

    @Override
    public V getAndRemove(String key) {
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
                Entry.of(e.key(), bytesToValue.apply(e.value(), null))));
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
    public Asset asset() {
        return asset;
    }

    @Override
    public KeyValueStore underlying() {
        return kvStore;
    }

    class TranslatingSubscriptionKVSCollection implements SubscriptionKVSCollection<String, V> {
        @Override
        public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
            Class eClass = rc.type();
            if (eClass == MapEvent.class) {
                Subscriber<MapEvent<String, V>> sub = (Subscriber<MapEvent<String, V>>) subscriber;

                Subscriber<MapEvent<String, BytesStore>> sub2 = e -> {
                    if (e.getClass() == InsertedEvent.class)
                        sub.onMessage(InsertedEvent.of(e.key(), bytesToValue.apply(e.value(), null)));
                    else if (e.getClass() == UpdatedEvent.class)
                        sub.onMessage(UpdatedEvent.of(e.key(), bytesToValue.apply(((UpdatedEvent<String, BytesStore>) e).oldValue(), null), bytesToValue.apply(e.value(), null)));
                    else
                        sub.onMessage(RemovedEvent.of(e.key(), bytesToValue.apply(e.value(), null)));
                };
                kvStore.subscription(true).registerSubscriber(rc, sub2);
            } else {
                kvStore.subscription(true).registerSubscriber(rc, subscriber);
            }
        }

        @Override
        public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
            kvStore.subscription(true).registerTopicSubscriber(rc, (T topic, BytesStore message) -> {
                subscriber.onMessage(topic, (E) bytesToValue.apply(message, null));
            });
            subscriptions.registerTopicSubscriber(rc, subscriber);
        }

        @Override
        public void notifyUpdate(String key, V oldValue, V value) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void notifyRemoval(String key, V oldValue) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public boolean hasSubscribers() {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void registerDownstream(RequestContext rc, Subscription subscription) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public void unregisterDownstream(RequestContext rc, Subscription subscription) {
            throw new UnsupportedOperationException("todo");
        }
    }
}
