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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer.notifyEachEvent;

public class ChronicleMapKeyValueStore<K, V> implements ObjectKeyValueStore<K, V>,
        Closeable {

    private static final ScheduledExecutorService DELAYED_CLOSER = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ChronicleMapKeyValueStore Closer", true));
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);

    private final ChronicleMap<K, V> chronicleMap;
    @NotNull
    private final ObjectSubscription<K, V> subscriptions;
    @NotNull
    private final Asset asset;
    @NotNull
    private final String assetFullName;
    @Nullable
    private final EventLoop eventLoop;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private Class<K> keyType;
    private Class<V> valueType;

    public ChronicleMapKeyValueStore(@NotNull RequestContext context, @NotNull Asset asset) {
        String basePath = context.basePath();
        keyType = context.keyType();
        valueType = context.valueType();
        double averageValueSize = context.getAverageValueSize();
        double averageKeySize = context.getAverageKeySize();
        long maxEntries = context.getEntries();
        this.asset = asset;
        this.assetFullName = asset.fullName();
        this.subscriptions = asset.acquireView(ObjectSubscription.class, context);
        this.subscriptions.setKvStore(this);
        this.eventLoop = asset.findOrCreateView(EventLoop.class);
        assert eventLoop != null;
        eventLoop.start();

        ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.of(context.keyType(), context.valueType());
        builder.putReturnsNull(context.putReturnsNull() != Boolean.FALSE)
                .removeReturnsNull(context.removeReturnsNull() != Boolean.FALSE);

        builder.entryOperations(new EntryOps());

        if (context.putReturnsNull() != Boolean.FALSE)
            builder.putReturnsNull(true);
        if (context.removeReturnsNull() != Boolean.FALSE)
            builder.removeReturnsNull(true);
        if (averageValueSize > 0)
            builder.averageValueSize(averageValueSize);
        else {
            LOG.warn("Using failsafe value size of 8. Most likely it's not the best fit for your use case, consider setting averageValueSize for this map: " + assetFullName);
            builder.averageValueSize(8);
        }
        if (averageKeySize > 0)
            builder.averageKeySize(averageKeySize);
        else {
            LOG.warn("Using failsafe key size of 8. Most likely it's not the best fit for your use case, consider setting averageKeySize for this map: " + assetFullName);
            builder.averageKeySize(8);
        }


        if (maxEntries > 0) builder.entries(maxEntries + 1); // we have to add a head room of 1
        else {
            LOG.warn("Using failsafe entries' number of 64k. Most likely it's not the best fit for your use case, and you might get runtime errors if you insert more than this number of entries. Consider setting maxEntries for this map: " + assetFullName);
            builder.entries(2 << 15);
        }

        if (basePath == null) {
            chronicleMap = builder.create();
        } else {
            @NotNull String pathname = basePath + "/" + context.name();
            //noinspection ResultOfMethodCallIgnored
            new File(basePath).mkdirs();
            try {
                chronicleMap = builder.createPersistedTo(new File(pathname));

            } catch (IOException e) {
                throw new IORuntimeException("Could not access " + pathname, e);
            }
        }
    }

    @NotNull
    @Override
    public KVSSubscription<K, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public boolean put(K key, V value) {
        try {
            return chronicleMap.put(key, value) != null;

        } catch (RuntimeException e) {
            if (LOG.isDebugEnabled())
                Jvm.debug().on(getClass(), "Failed to write " + key + ", " + value, e);
            throw e;
        }
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        if (!isClosed.get())
            return chronicleMap.put(key, value);
        else
            return null;
    }

    @Override
    public boolean remove(K key) {
        return chronicleMap.remove(key) != null;
    }

    @Nullable
    @Override
    public V getAndRemove(K key) {

        if (!isClosed.get())
            return chronicleMap.remove(key);
        else
            return null;
    }

    @Override
    public V getUsing(K key, @Nullable Object value) {
        if (value != null)
            throw new UnsupportedOperationException("Mutable values not supported");
        return chronicleMap.getUsing(key, (V) value);
    }

    @Override
    public long longSize() {
        return chronicleMap.size();
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<K> kConsumer) throws
            InvalidSubscriberException {
        //Ignore the segments and return keysFor the whole map
        notifyEachEvent(chronicleMap.keySet(), kConsumer);
    }

    @Override
    public void entriesFor(int segment,
                           @NotNull SubscriptionConsumer<MapEvent<K, V>> kvConsumer) {
        //Ignore the segments and return entriesFor the whole map
        chronicleMap.entrySet().stream()
                .map(e -> InsertedEvent.of(assetFullName, e.getKey(), e.getValue()))
                .forEach(ThrowingConsumer.asConsumer(kvConsumer::accept));
    }

    @NotNull
    @Override
    public Iterator<Map.Entry<K, V>> entrySetIterator() {
        return chronicleMap.entrySet().iterator();
    }

    @NotNull
    @Override
    public Iterator<K> keySetIterator() {
        return chronicleMap.keySet().iterator();
    }

    @Override
    public void clear() {
        chronicleMap.clear();
    }

    @Override
    public boolean containsValue(final V value) {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public KeyValueStore<K, V> underlying() {
        return null;
    }

    @Override
    public void close() {
        isClosed.set(true);
        assert eventLoop != null;
        eventLoop.stop();
        closeQuietly(asset.findView(TcpChannelHub.class));
        DELAYED_CLOSER.schedule(() -> Closeable.closeQuietly(chronicleMap), 1, TimeUnit.SECONDS);
    }

    @Override
    public Class<K> keyType() {
        return keyType;
    }

    @Override
    public Class<V> valueType() {
        return valueType;
    }

    private class EntryOps implements MapEntryOperations<K, V, Void> {
        @Override
        public Void remove(@NotNull MapEntry<K, V> entry) {
            subscriptions.notifyEvent(RemovedEvent.of(assetFullName, entry.key().get(), entry.value().get()));
            return MapEntryOperations.super.remove(entry);
        }

        @Override
        public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
            subscriptions.notifyEvent(UpdatedEvent.of(assetFullName, entry.key().get(), entry.value().get(),
                    newValue.get(), true));
            return MapEntryOperations.super.replaceValue(entry, newValue);
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
            subscriptions.notifyEvent(InsertedEvent.of(assetFullName, absentEntry.absentKey().get(), value.get()));
            return MapEntryOperations.super.insert(absentEntry, value);
        }
    }
}
