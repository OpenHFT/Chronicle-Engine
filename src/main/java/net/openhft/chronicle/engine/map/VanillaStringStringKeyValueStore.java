/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.*;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static net.openhft.chronicle.engine.map.Buffers.BUFFERS;

/**
 * Created by peter on 25/05/15.
 */
public class VanillaStringStringKeyValueStore implements StringStringKeyValueStore {
    @NotNull
    private final ObjectKVSSubscription<String, String> subscriptions;

    private final SubscriptionKeyValueStore<String, BytesStore> kvStore;
    private final Asset asset;

    public VanillaStringStringKeyValueStore(RequestContext context, @NotNull Asset asset,
                                            @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore) throws AssetNotFoundException {
        this(asset.acquireView(ObjectKVSSubscription.class, context), asset, kvStore);
    }

    VanillaStringStringKeyValueStore(@NotNull ObjectKVSSubscription<String, String> subscriptions,
                                     @NotNull Asset asset,
                                     @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore) throws AssetNotFoundException {
        this.asset = asset;
        this.kvStore = kvStore;
        asset.registerView(ValueReader.class, StringValueReader.BYTES_STORE_TO_STRING);
        RawKVSSubscription<String, BytesStore> rawSubscription =
                (RawKVSSubscription<String, BytesStore>) kvStore.subscription(true);
        this.subscriptions = subscriptions;
        subscriptions.setKvStore(this);
        rawSubscription.registerDownstream(mpe ->
                subscriptions.notifyEvent(mpe.translate(s -> s, BytesStoreToString.BYTES_STORE_TO_STRING)));
    }

    @NotNull
    @Override
    public Class<String> keyType() {
        return String.class;
    }

    @NotNull
    @Override
    public Class<String> valueType() {
        return String.class;
    }

    @NotNull
    @Override
    public ObjectKVSSubscription<String, String> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public boolean put(String key, @NotNull String value) {
        Buffers b = BUFFERS.get();
        Bytes<ByteBuffer> bytes = b.valueBuffer;
        bytes.clear();
        bytes.append8bit(value);
        return kvStore.put(key, bytes);
    }

    @Nullable
    @Override
    public String getAndPut(String key, @NotNull String value) {
        Buffers b = BUFFERS.get();
        Bytes<ByteBuffer> bytes = b.valueBuffer;
        bytes.clear();
        bytes.append(value);
        BytesStore retBytes = kvStore.getAndPut(key, bytes);
        if (retBytes == null) return null;
        else {
            String s = retBytes.toString();
            retBytes.release();
            return s;
        }
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
    public String getUsing(String key, Object value) {
        Buffers b = BUFFERS.get();
        BytesStore retBytes = kvStore.getUsing(key, b.valueBuffer);
        return retBytes == null ? null : retBytes.toString();
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
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<String, String>> kvConsumer) throws InvalidSubscriberException {
        kvStore.entriesFor(segment, e -> kvConsumer.accept(e.translate(k -> k, BytesStoreToString.BYTES_STORE_TO_STRING)));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<String, String>> entrySetIterator() {
        List<Map.Entry<String, String>> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                entriesFor(i, e -> entries.add(new SimpleEntry<>(e.key(), e.value())));
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
    public boolean containsValue(final String value) {
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
    public void close() {
        kvStore.close();
    }

    @Override
    public void accept(final ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }

    enum BytesStoreToString implements Function<BytesStore, String> {
        BYTES_STORE_TO_STRING;

        @Nullable
        @Override
        public String apply(@Nullable BytesStore bs) {
            return bs == null ? null : BytesUtil.to8bitString(bs);
        }
    }

    enum StringValueReader implements ValueReader<BytesStore, String> {
        BYTES_STORE_TO_STRING;

        @Nullable
        @Override
        public String apply(@Nullable BytesStore bs) {
            return bs == null ? null : BytesUtil.to8bitString(bs);
        }
    }
}
