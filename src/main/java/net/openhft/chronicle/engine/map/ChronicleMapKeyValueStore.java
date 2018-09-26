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
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.ChronicleMapCfg;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.EngineCluster;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.network.cluster.HostDetails;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.chronicle.enterprise.map.ReplicatedMap;
import software.chronicle.enterprise.map.ReplicationEventListener;
import software.chronicle.enterprise.map.cluster.MapCluster;
import software.chronicle.enterprise.map.cluster.MapClusterContext;
import software.chronicle.enterprise.map.config.MapReplicationCfg;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

    public ChronicleMapKeyValueStore(@NotNull ChronicleMapCfg<K, V> config, @NotNull Asset asset) {
        this.asset = asset;
        this.assetFullName = asset.fullName();
        this.subscriptions = asset.acquireView(ObjectSubscription.class, RequestContext.requestContext(assetFullName));
        this.subscriptions.setKvStore(this);
        this.eventLoop = asset.findOrCreateView(EventLoop.class);


        Asset root = asset.root();
        byte localHostId = HostIdentifier.localIdentifier(root);
        String name = config.name();
        if (localHostId > 0) {
            // we run replicated!
            // try to locate existing cluster
            ReplicatedMap replicatedMap;

            replicatedMap = root.getView(ReplicatedMap.class);
            if (replicatedMap == null) {
                Clusters clusters = root.acquireView(Clusters.class);
                EngineCluster engineCluster = clusters.firstCluster();
                Map<String, HostDetails> origHostDetails = engineCluster.hostDetails;

                // need to validate cluster first
                Map<String, String> replicationMapping = config.replicationHostMapping();
                if (replicationMapping == null || replicationMapping.isEmpty())
                    throw new IllegalStateException("Replicated map is not configured correctly, replication host mapping is missing: " + assetFullName);
                if (!origHostDetails.keySet().containsAll(replicationMapping.keySet()))
                    throw new IllegalStateException("Replicated map is not configured correctly, replication host mapping is invalid: " + assetFullName);

                replicatedMap = createReplicatedMap(engineCluster, localHostId, replicationMapping);
                root.addView(ReplicatedMap.class, replicatedMap);
            }

            replicatedMap.addReplicatedMap(config, localHostId);

            chronicleMap = replicatedMap.getMap(name);
            replicatedMap.addReplicationListener(name, new ReplicationEventListener<K, V>() {
                @Override
                public void removed(Data<K> key, Data<V> value) {
                    subscriptions.notifyEvent(RemovedEvent.of(assetFullName, key.get(), value.get()));
                }

                @Override
                public void updated(Data<K> key, Data<V> oldValue, Data<V> newValue) {
                    if (oldValue == null)
                        subscriptions.notifyEvent(InsertedEvent.of(assetFullName, key.get(), newValue.get()));
                    else
                        subscriptions.notifyEvent(UpdatedEvent.of(assetFullName, key.get(), oldValue.get(), newValue.get()));
                }
            });
        } else {
            // local
            ChronicleMapBuilder<K, V> builder;
            try {
                builder = config.mapBuilder(localHostId);
            } catch (IllegalStateException e) {
                throw new IllegalArgumentException("Invalid map config for " + assetFullName, e);
            }

            if (config.mapFileDataDirectory() == null) {
                chronicleMap = builder.create();
            } else {
                @NotNull String pathname = config.mapFileDataDirectory() + "/" + name;
                //noinspection ResultOfMethodCallIgnored
                new File(config.mapFileDataDirectory()).mkdirs();
                try {
                    chronicleMap = builder.createOrRecoverPersistedTo(new File(pathname), false);


                } catch (IOException e) {
                    throw new IORuntimeException("Could not access " + pathname, e);
                }
            }
            ((VanillaChronicleMap) chronicleMap).entryOperations = new EntryOps();
        }

        this.keyType = chronicleMap.keyClass();
        this.valueType = chronicleMap.valueClass();

        assert eventLoop != null;
        eventLoop.start();
    }

    @NotNull
    private ReplicatedMap createReplicatedMap(EngineCluster engineCluster, byte localHostId, Map<String, String> replicationMapping) {
        MapCluster cluster = new MapCluster(ObjectUtils.newInstance(MapClusterContext.class));

        // HACKERY POCKERY
        engineCluster.hostDetails.forEach((name, hd) -> {
            HostDetails hd0 = new HostDetails();
            hd0.hostId(hd.hostId());
            hd0.tcpBufferSize(hd.tcpBufferSize());
            hd0.connectUri(replicationMapping.get(name));
            cluster.hostDetails.put(name, hd0);
        });

        EngineClusterContext engineCtx = engineCluster.clusterContext();
        assert engineCtx != null;
        cluster.context().heartbeatIntervalMs(engineCtx.heartbeatIntervalMs());
        cluster.context().heartbeatTimeoutMs(engineCtx.heartbeatTimeoutMs());

        MapReplicationCfg config = new MapReplicationCfg(cluster, new LinkedHashMap<>());
        return new ReplicatedMap(config, localHostId);
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
            subscriptions.notifyEvent(UpdatedEvent.of(assetFullName, entry.key().get(), entry.value().get(), newValue.get()));
            return MapEntryOperations.super.replaceValue(entry, newValue);
        }

        @Override
        public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V> value) {
            subscriptions.notifyEvent(InsertedEvent.of(assetFullName, absentEntry.absentKey().get(), value.get()));
            return MapEntryOperations.super.insert(absentEntry, value);
        }
    }
}
