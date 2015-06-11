package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

import static net.openhft.chronicle.engine.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class VanillaStringStringKeyValueStore implements StringStringKeyValueStore {
    public static final Function<BytesStore, String> BYTES_STORE_STRING_FUNCTION = v -> BytesUtil.to8bitString(v);
    private final ObjectSubscription<String, StringBuilder, String> subscriptions;
    private final Function<BytesStore, String> bytesToValue;
    private SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore;
    private Asset asset;

    public VanillaStringStringKeyValueStore(RequestContext context, @NotNull Asset asset,
                                            @NotNull SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore) throws AssetNotFoundException {
        this(asset.acquireView(ObjectSubscription.class, context), asset, kvStore);
    }

    VanillaStringStringKeyValueStore(ObjectSubscription<String, StringBuilder, String> subscriptions,
                                     @NotNull Asset asset,
                                     @NotNull SubscriptionKeyValueStore<String, Bytes, BytesStore> kvStore) throws AssetNotFoundException {
        this.asset = asset;
        this.kvStore = kvStore;
        bytesToValue = b -> b == null ? null : b.toString();
        asset.registerView(ValueReader.class, (ValueReader<BytesStore, String>) (bs, v) ->
                bytesToValue.apply(bs));
        RawSubscription<String, Bytes, BytesStore> rawSubscription =
                (RawSubscription<String, Bytes, BytesStore>) kvStore.subscription(true);
        this.subscriptions = subscriptions;
        rawSubscription.registerDownstream(mpe ->
                subscriptions.notifyEvent(mpe.translate(s -> s, bytesToValue)));
    }

    @NotNull
    @Override
    public ObjectSubscription<String, StringBuilder, String> subscription(boolean createIfAbsent) {
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
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<String, String>> kvConsumer) throws InvalidSubscriberException {
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
}
