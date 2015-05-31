package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine2.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class ObjectObjectKeyValueStore<K, MV extends V, V> implements KeyValueStore<K, MV, V> {
    private final BiFunction<K, Bytes, Bytes> keyToBytes;
    private final BiFunction<V, Bytes, Bytes> valueToBytes;
    private final BiFunction<BytesStore, K, K> bytesToKey;
    private final BiFunction<BytesStore, V, V> bytesToValue;
    private KeyValueStore<BytesStore, Bytes, BytesStore> kvStore;
    private Asset asset;

    public ObjectObjectKeyValueStore(RequestContext context, Asset asset, Assetted assetted) {
        this.asset = asset;
        Class type = context.type();
        keyToBytes = toBytes(type);
        bytesToKey = fromBytes(type);
        Class type2 = context.type2();
        valueToBytes = toBytes(type2);
        bytesToValue = fromBytes(type2);
        kvStore = (KeyValueStore<BytesStore, Bytes, BytesStore>) assetted;
    }

    static <T> BiFunction<T, Bytes, Bytes> toBytes(Class type) {
        if (type == String.class)
            return (t, bytes) -> (Bytes) bytes.append((String) t);
        throw new UnsupportedOperationException("todo");
    }

    private <T> BiFunction<BytesStore, T, T> fromBytes(Class type) {
        if (type == String.class)
            return (t, bytes) -> (T) (bytes == null ? null : bytes.toString());
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public V getAndPut(K key, V value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        Bytes valueBytes = valueToBytes.apply(value, b.valueBuffer);
        BytesStore retBytes = kvStore.getAndPut(keyBytes, valueBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public V getAndRemove(K key) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        BytesStore retBytes = kvStore.getAndRemove(keyBytes);
        return retBytes == null ? null : bytesToValue.apply(retBytes, null);
    }

    @Override
    public V getUsing(K key, MV value) {
        Buffers b = BUFFERS.get();
        Bytes keyBytes = keyToBytes.apply(key, b.keyBuffer);
        BytesStore retBytes = kvStore.getUsing(keyBytes, b.valueBuffer);
        return retBytes == null ? null : bytesToValue.apply(retBytes, value);
    }

    @Override
    public long size() {
        return kvStore.size();
    }

    @Override
    public void keysFor(int segment, Consumer<K> kConsumer) {
        kvStore.keysFor(segment, k -> kConsumer.accept(bytesToKey.apply(k, null)));
    }

    @Override
    public void entriesFor(int segment, Consumer<Entry<K, V>> kvConsumer) {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(
                Entry.of(
                        bytesToKey.apply(e.key(), null),
                        bytesToValue.apply(e.value(), null))));
    }

    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        kvStore.clear();
    }

    @Override
    public V replace(K key, V value) {
        throw new UnsupportedOperationException("todo");
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
    public View forSession(LocalSession session, Asset asset) {
        throw new UnsupportedOperationException("todo");
    }
}
