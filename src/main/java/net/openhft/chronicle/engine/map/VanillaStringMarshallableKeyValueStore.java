package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.ClassLocal;
import net.openhft.chronicle.engine.api.map.*;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static net.openhft.chronicle.engine.map.Buffers.BUFFERS;

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
    private final ObjectKVSSubscription<String, V, V> subscriptions;
    private SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore;
    private Asset asset;

    public VanillaStringMarshallableKeyValueStore(RequestContext context, Asset asset,
                                                  SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore) throws AssetNotFoundException {
        this(asset.acquireView(ObjectKVSSubscription.class, context), asset, context.type2(),
                kvStore, context.wireType());
    }

    VanillaStringMarshallableKeyValueStore(ObjectKVSSubscription<String, V, V> subscriptions, Asset asset, Class type2,
                                           SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore,
                                           Function<Bytes, Wire> wireType) {
        this.asset = asset;
        valueToBytes = toBytes(type2, wireType);
        bytesToValue = fromBytes(type2, wireType);
        this.kvStore = kvStore;
        asset.registerView(ValueReader.class, (ValueReader<BytesStore, V>) (bs, v) ->
                bytesToValue.apply(bs, null));
        RawKVSSubscription<String, Bytes, BytesStore> rawSubscription =
                (RawKVSSubscription<String, Bytes, BytesStore>) kvStore.subscription(true);
        this.subscriptions = subscriptions;
        rawSubscription.registerDownstream(mpe ->
                subscriptions.notifyEvent(mpe.translate(s -> s, b -> bytesToValue.apply(b, null))));
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
                if (bytes == null)
                    return null;

                t = acquireInstance(type, t);
                ((Marshallable) t).readMarshallable(wireType.apply(bytes.bytes()));
                if (bytes instanceof Bytes)
                    ((Bytes) bytes).position(0);

                return t;
            };
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public ObjectKVSSubscription<String, V, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public boolean put(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        return kvStore.put(key, valueBytes);
    }

    @Override
    public V getAndPut(String key, V value) {
        Buffers b = BUFFERS.get();
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        BytesStore retBytes = kvStore.getAndPut(key, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public boolean remove(String key) {
        return kvStore.remove(key);
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
    public long longSize() {
        return kvStore.longSize();
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<String> kConsumer) throws InvalidSubscriberException {
        kvStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapEvent<String, V>> kvConsumer) throws InvalidSubscriberException {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(
                InsertedEvent.of(asset.fullName(), e.key(), bytesToValue.apply(e.value(), null))));
    }

    @Override
    public Iterator<Map.Entry<String, V>> entrySetIterator() {
        // todo optimise
        List<Map.Entry<String, V>> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                entriesFor(i, e -> entries.add(new AbstractMap.SimpleEntry<>(e.key(), e.value())));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
        return entries.iterator();
    }

    @Override
    public Iterator<String> keySetIterator() {
        // todo optimise
        List<String> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                keysFor(i, k -> entries.add(k));
        } catch (InvalidSubscriberException e) {
            throw new AssertionError(e);
        }
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

    @Override
    public void close() {
        kvStore.close();
    }
}
