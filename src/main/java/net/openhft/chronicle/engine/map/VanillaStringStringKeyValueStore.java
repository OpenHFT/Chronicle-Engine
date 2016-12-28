/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
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
    private final ObjectSubscription<String, String> subscriptions;

    @NotNull
    private final SubscriptionKeyValueStore<String, BytesStore> kvStore;
    @NotNull
    private final Asset asset;

    public VanillaStringStringKeyValueStore(RequestContext context, @NotNull Asset asset,
                                            @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore) throws AssetNotFoundException {
        this(asset.acquireView(ObjectSubscription.class, context), asset, kvStore);
    }

    VanillaStringStringKeyValueStore(@NotNull ObjectSubscription<String, String> subscriptions,
                                     @NotNull Asset asset,
                                     @NotNull SubscriptionKeyValueStore<String, BytesStore> kvStore) throws AssetNotFoundException {
        this.asset = asset;
        this.kvStore = kvStore;
        asset.registerView(ValueReader.class, StringValueReader.BYTES_STORE_TO_STRING);
        @NotNull RawKVSSubscription<String, BytesStore> rawSubscription =
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
    public ObjectSubscription<String, String> subscription(boolean createIfAbsent) {
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
        bytes.appendUtf8(value);
        @Nullable BytesStore retBytes = kvStore.getAndPut(key, bytes);
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
        @Nullable BytesStore retBytes = kvStore.getAndRemove(key);
        return retBytes == null ? null : retBytes.toString();
    }

    @Nullable
    @Override
    public String getUsing(String key, Object value) {
        Buffers b = BUFFERS.get();
        @Nullable BytesStore retBytes = kvStore.getUsing(key, b.valueBuffer);
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
        @NotNull List<Map.Entry<String, String>> entries = new ArrayList<>();
        try {
            for (int i = 0, seg = segments(); i < seg; i++)
                entriesFor(i, entries::add);
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

    @NotNull
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
            return bs == null ? null : bs.to8bitString();
        }
    }

    enum StringValueReader implements ValueReader<BytesStore, String> {
        BYTES_STORE_TO_STRING;

        @Nullable
        @Override
        public String apply(@Nullable BytesStore bs) {
            return bs == null ? null : bs.to8bitString();
        }
    }
}
