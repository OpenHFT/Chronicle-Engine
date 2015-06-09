package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.StreamingDataInput;
import net.openhft.chronicle.core.ClassLocal;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

import static net.openhft.chronicle.engine.map.Buffers.BUFFERS;

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
    public static final Function<BytesStore, String> BYTES_STORE_STRING_FUNCTION = v -> BytesUtil.to8bitString((StreamingDataInput) v);
    private final SubscriptionKVSCollection<String, StringBuilder, String> subscriptions = new TranslatingSubscriptionKVSCollection();
    private SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore;
    private Asset asset;


    public VanillaStringStringKeyValueStore(RequestContext context, @NotNull Asset asset, @NotNull ThrowingSupplier<Assetted, AssetNotFoundException> kvStore) throws AssetNotFoundException {
        this.asset = asset;
        this.kvStore = (SubscriptionKeyValueStore<String, Bytes, BytesStore>) kvStore.get();
        asset.registerView(ValueReader.class, (ValueReader<BytesStore, String>) (bs, s) -> BytesUtil.to8bitString((StreamingDataInput) bs));
    }

    @Nullable
    static <T> T acquireInstance(@NotNull Class type, @Nullable T t) {
        if (t == null)
            try {
                t = (T) CONSTRUCTORS.get(type).newInstance();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        return t;
    }

    @NotNull
    @Override
    public SubscriptionKVSCollection<String, StringBuilder, String> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public boolean put(String key, String value) {
        Buffers b = BUFFERS.get();
        Bytes<ByteBuffer> bytes = b.valueBuffer;
        bytes.clear();
        bytes.append8bit(value);
        bytes.flip();
        return kvStore.put(key, bytes);
    }

    @Nullable
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
    public boolean remove(String key) {
        return kvStore.remove(key);
    }

    @Nullable
    @Override
    public String getAndRemove(String key) {
        BytesStore retBytes = kvStore.getAndRemove(key);
        return retBytes == null ? null : retBytes.toString();
    }

    @Nullable
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
    public void keysFor(int segment, SubscriptionConsumer<String> kConsumer) throws InvalidSubscriberException {
        kvStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapReplicationEvent<String, String>> kvConsumer) throws InvalidSubscriberException {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(e.translate(k -> k, BYTES_STORE_STRING_FUNCTION)));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<String, String>> entrySetIterator() {
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                entriesFor(i, e -> entries.add(new AbstractMap.SimpleEntry<>(e.key(), e.value())));
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

    class TranslatingSubscriptionKVSCollection implements SubscriptionKVSCollection<String, StringBuilder, String> {
        @NotNull
        SubscriptionKVSCollection<String, StringBuilder, String> subscriptions = new VanillaSubscriptionKVSCollection<>(VanillaStringStringKeyValueStore.this);

        @Override
        public <E> void registerSubscriber(@NotNull RequestContext rc, Subscriber<E> subscriber) {
            Class eClass = rc.type();
            if (eClass == MapEvent.class) {
                Subscriber<MapReplicationEvent<String, String>> sub = (Subscriber) subscriber;

                Subscriber<MapReplicationEvent<String, BytesStore>> sub2 = e ->
                        sub.onMessage(e.translate(k -> k, BYTES_STORE_STRING_FUNCTION));
                kvStore.subscription(true).registerSubscriber(rc, sub2);
            } else {
                subscriptions.registerSubscriber(rc, subscriber);
            }
        }

        @Override
        public <T, E> void registerTopicSubscriber(RequestContext rc, @NotNull TopicSubscriber<T, E> subscriber) {
            kvStore.subscription(true).registerTopicSubscriber(rc, (T topic, E message) -> {
                subscriber.onMessage(topic, (E) BytesUtil.to8bitString((StreamingDataInput) message));
            });
            subscriptions.registerTopicSubscriber(rc, subscriber);
        }

        @Override
        public void notifyEvent(MapReplicationEvent<String, String> mpe) throws InvalidSubscriberException {
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
        public void registerDownstream(Subscription subscription) {
            throw new UnsupportedOperationException("todo");
        }

        @Override
        public boolean needsPrevious() {
            SubscriptionKVSCollection<String, Bytes, BytesStore> subs = kvStore.subscription(false);
            return subs != null && subs.needsPrevious();
        }

        @Override
        public void setKvStore(KeyValueStore<String, StringBuilder, String> store) {
            throw new UnsupportedOperationException("todo");
        }
    }
}
