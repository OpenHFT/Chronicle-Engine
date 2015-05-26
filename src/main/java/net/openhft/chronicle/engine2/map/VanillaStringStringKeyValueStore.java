package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.ClassLocal;
import net.openhft.chronicle.core.util.StringUtils;
import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.TopicSubscriber;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.StringStringKeyValueStore;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine2.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class VanillaStringStringKeyValueStore implements StringStringKeyValueStore {
    private static final ClassLocal<Constructor> CONSTRUCTORS = ClassLocal.withInitial(c -> {
        try {
            Constructor con = c.getDeclaredConstructor();
            con.setAccessible(true);
            return con;
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    });
    private final SubscriptionKVSCollection<String, StringBuilder, String> subscriptions = new SubscriptionKVSCollection<>(this);
    private SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore;
    private Asset asset;


    public VanillaStringStringKeyValueStore(FactoryContext<SubscriptionKeyValueStore<String, Bytes, BytesStore>> context) {
        asset = context.parent();
        kvStore = context.item();
    }

    static <T> BiFunction<T, Bytes, Bytes> toBytes(FactoryContext context, Class type) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.append((String) t);
        if (Marshallable.class.isAssignableFrom(type))
            return (t, bytes) -> {
                t = acquireInstance(type, t);
                ((Marshallable) t).writeMarshallable((WireOut) context.wireType().apply(bytes));
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

    private <T> BiFunction<BytesStore, T, T> fromBytes(FactoryContext context, Class type) {
        if (type == String.class)
            return (t, bytes) -> (T) (bytes == null ? null : bytes.toString());
        if (Marshallable.class.isAssignableFrom(type))
            return (bytes, t) -> {
                t = acquireInstance(type, t);
                ((Marshallable) t).readMarshallable((WireIn) context.wireType().apply(bytes));
                ((Bytes) bytes).position(0);
                return t;
            };
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void put(String key, String value) {
        Buffers b = BUFFERS.get();
        Bytes<ByteBuffer> bytes = b.valueBuffer;
        bytes.clear();
        bytes.append(value);
        bytes.flip();
        kvStore.put(key, bytes);
    }

    @Override
    public String getAndPut(String key, String value) {
        Buffers b = BUFFERS.get();
        Bytes<ByteBuffer> bytes = b.valueBuffer;
        bytes.clear();
        bytes.append(value);
        bytes.flip();
        BytesStore retBytes = kvStore.getAndPut(key, bytes);
        return retBytes == null ? null : retBytes.toString();
    }

    @Override
    public void remove(String key) {
        kvStore.remove(key);
    }

    @Override
    public String getAndRemove(String key) {
        BytesStore retBytes = kvStore.getAndRemove(key);
        return retBytes == null ? null : retBytes.toString();
    }

    @Override
    public String getUsing(String key, StringBuilder value) {
        Buffers b = BUFFERS.get();
        BytesStore retBytes = kvStore.getUsing(key, b.valueBuffer);
        return retBytes == null ? null : retBytes.toString();
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
    public void entriesFor(int segment, Consumer<Entry<String, String>> kvConsumer) {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(
                Entry.of(e.key(), e.value().toString())));
    }

    @Override
    public Iterator<Map.Entry<String, String>> entrySetIterator() {
        List<Map.Entry<String, String>> entries = new ArrayList<>();
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
    public void underlying(KeyValueStore<String, StringBuilder, String> underlying) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public KeyValueStore underlying() {
        return kvStore;
    }

    @Override
    public <E> void registerSubscriber(Class<E> eClass, Subscriber<E> subscriber, String query) {
        if (eClass == MapEvent.class) {
            Subscriber<MapEvent<String, String>> sub = (Subscriber<MapEvent<String, String>>) subscriber;

            kvStore.registerSubscriber((Class<MapEvent<String, BytesStore>>) eClass, e -> {
                if (e.getClass() == InsertedEvent.class)
                    sub.on(InsertedEvent.of(e.key(), e.value().toString()));
                else if (e.getClass() == UpdatedEvent.class)
                    sub.on(UpdatedEvent.of(e.key(), ((UpdatedEvent<String, BytesStore>) e).oldValue().toString(), e.value().toString()));
                else
                    sub.on(RemovedEvent.of(e.key(), e.value().toString()));
            }, query);
        }
        subscriptions.registerSubscriber(eClass, subscriber, query);
    }

    @Override
    public <T, E> void registerTopicSubscriber(Class<T> tClass, Class<E> eClass, TopicSubscriber<T, E> subscriber, String query) {
        kvStore.registerTopicSubscriber(tClass, eClass, (topic, message) -> {
            subscriber.onMessage(topic, (E) StringUtils.toString(message));
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
}
