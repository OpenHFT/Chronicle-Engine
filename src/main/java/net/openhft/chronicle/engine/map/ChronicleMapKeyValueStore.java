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

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.fs.Cluster;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.HostDetails;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.hash.replication.EngineReplicationLangBytesConsumer;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.lang.io.Bytes;
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
import java.util.function.Supplier;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer.notifyEachEvent;
import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;
import static net.openhft.chronicle.network.VanillaSessionDetails.of;

public class ChronicleMapKeyValueStore<K, MV, V> implements ObjectKeyValueStore<K, V>,
        Closeable, Supplier<EngineReplication> {

    private static final ScheduledExecutorService DELAYED_CLOSER = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("ChronicleMapKeyValueStore Closer", true));
    private static final Logger LOG = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
    private final ChronicleMap<K, V> chronicleMap;
    @NotNull
    private final ObjectSubscription<K, V> subscriptions;
    @Nullable
    private final EngineReplication engineReplicator;
    @NotNull
    private final Asset asset;
    @NotNull
    private final String assetFullName;
    @Nullable
    private final EventLoop eventLoop;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private Class keyType;
    private Class valueType;

    public ChronicleMapKeyValueStore(@NotNull RequestContext context, @NotNull Asset asset) {
        String basePath = context.basePath();
        keyType = context.keyType();
        valueType = context.valueType();
        double averageValueSize = context.getAverageValueSize();
        long maxEntries = context.getEntries();
        this.asset = asset;
        this.assetFullName = asset.fullName();
        this.subscriptions = asset.acquireView(ObjectSubscription.class, context);
        this.subscriptions.setKvStore(this);
        this.eventLoop = asset.findOrCreateView(EventLoop.class);
        assert eventLoop != null;
        eventLoop.start();

        ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.of(context.keyType(), context.valueType());
        HostIdentifier hostIdentifier = null;
        EngineReplication engineReplicator1 = null;
        try {
            engineReplicator1 = asset.acquireView(EngineReplication.class);

            final EngineReplicationLangBytesConsumer langBytesConsumer = asset.findView
                    (EngineReplicationLangBytesConsumer.class);

            hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
            assert hostIdentifier != null;
            builder.putReturnsNull(context.putReturnsNull() != Boolean.FALSE)
                    .removeReturnsNull(context.removeReturnsNull() != Boolean.FALSE);
            builder.replication(builder().engineReplication(langBytesConsumer)
                    .createWithId(hostIdentifier.hostId()));

        } catch (AssetNotFoundException anfe) {
            if (LOG.isDebugEnabled())
                LOG.debug("replication not enabled " + anfe.getMessage());
        }

        this.engineReplicator = engineReplicator1;

        Boolean nullOldValueOnUpdateEvent = context.nullOldValueOnUpdateEvent();
        if (nullOldValueOnUpdateEvent != null && nullOldValueOnUpdateEvent) {
            builder.bytesEventListener(new NullOldValuePublishingOperations());
        } else {
            builder.eventListener(new PublishingOperations());
        }

        if (context.putReturnsNull() != Boolean.FALSE)
            builder.putReturnsNull(true);
        if (context.removeReturnsNull() != Boolean.FALSE)
            builder.removeReturnsNull(true);
        if (averageValueSize > 0)
            builder.averageValueSize(averageValueSize);
        if (maxEntries > 0)
            builder.entries(maxEntries);

        if (basePath == null)
            chronicleMap = builder.create();
        else {
            String pathname = basePath + "/" + context.name();
            //noinspection ResultOfMethodCallIgnored
            new File(basePath).mkdirs();
            try {
                chronicleMap = builder.createPersistedTo(new File(pathname));
            } catch (IOException e) {
                IORuntimeException iore = new IORuntimeException("Could not access " + pathname);
                iore.initCause(e);
                throw iore;
            }
        }

        if (hostIdentifier != null) {
            Clusters clusters = asset.findView(Clusters.class);

            if (clusters == null) {
                LOG.warn("no clusters found.");
                return;
            }

            final Cluster cluster = clusters.get(context.cluster());

            if (cluster == null) {
                LOG.warn("no cluster found name=" + context.cluster());
                return;
            }

            byte localIdentifier = hostIdentifier.hostId();

            if (LOG.isDebugEnabled())
                LOG.debug("hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + cluster.hostDetails());

            for (HostDetails hostDetails : cluster.hostDetails()) {

                // its the identifier with the larger values that will establish the connection
                int remoteIdentifier = hostDetails.hostId;

                if (remoteIdentifier <= localIdentifier) {

                    if (LOG.isDebugEnabled())
                        LOG.debug("skipping : attempting to connect to localIdentifier=" +
                                localIdentifier + ", remoteIdentifier=" + remoteIdentifier);

                    continue;
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("attempting to connect to localIdentifier=" + localIdentifier + ", " +
                            "remoteIdentifier=" + remoteIdentifier);

                final SessionDetails sessionDetails = of("replicationNode", "<token>", "<domain>");
                final TcpChannelHub tcpChannelHub = hostDetails.acquireTcpChannelHub(asset, eventLoop, context.wireType(), sessionDetails);
                final ReplicationHub replicationHub = new ReplicationHub(context, tcpChannelHub, eventLoop, isClosed);
                replicationHub.bootstrap(engineReplicator1, localIdentifier, (byte) remoteIdentifier);
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
        return chronicleMap.update(key, value) != UpdateResult.INSERT;
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
        if (value != null) throw new UnsupportedOperationException("Mutable values not supported");
        return chronicleMap.getUsing(key, (V) value);
    }

    @Override
    public long longSize() {
        return chronicleMap.size();
    }

    @Override
    public void keysFor(int segment, @NotNull SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        //Ignore the segments and return keysFor the whole map
        notifyEachEvent(chronicleMap.keySet(), kConsumer);
    }

    @Override
    public void entriesFor(int segment, @NotNull SubscriptionConsumer<MapEvent<K, V>> kvConsumer) throws InvalidSubscriberException {
        //Ignore the segments and return entriesFor the whole map
        chronicleMap.entrySet().stream().map(e -> InsertedEvent.of(assetFullName, e.getKey(), e.getValue(), false)).forEach(e -> {
            try {
                kvConsumer.accept(e);
            } catch (InvalidSubscriberException t) {
                throw Jvm.rethrow(t);
            }
        });
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
    public void accept(@NotNull final ReplicationEntry replicationEntry) {
        if (!isClosed.get() && engineReplicator != null)
            engineReplicator.applyReplication(replicationEntry);
        else
            LOG.warn("message skipped as closed replicationEntry=" + replicationEntry);
    }

    @Nullable
    @Override
    public EngineReplication get() {
        return engineReplicator;
    }

    @Override
    public Class<K> keyType() {
        return keyType;
    }

    @Override
    public Class<V> valueType() {
        return valueType;
    }

    private class PublishingOperations extends MapEventListener<K, V> {
        @Override
        public void onRemove(@NotNull K key, V value, boolean replicationEven) {
            if (subscriptions.hasSubscribers())
                subscriptions.notifyEvent(RemovedEvent.of(assetFullName, key, value, false));
        }

        @Override
        public void onPut(@NotNull K key, V newValue, @Nullable V replacedValue,
                          boolean replicationEvent, boolean added) {
            if (subscriptions.hasSubscribers())
                if (added) {
                    subscriptions.notifyEvent(InsertedEvent.of(assetFullName, key, newValue, replicationEvent));
                } else {
                    subscriptions.notifyEvent(UpdatedEvent.of(assetFullName, key, replacedValue, newValue, replicationEvent));
                }
        }
    }

    private class NullOldValuePublishingOperations extends BytesMapEventListener {
        @Override
        public void onRemove(Bytes entry, long metaDataPos, long keyPos, long valuePos,
                             boolean replicationEvent) {
            if (subscriptions.hasSubscribers()) {
                K key = chronicleMap.readKey(entry, keyPos);
                V value = chronicleMap.readValue(entry, valuePos);
                subscriptions.notifyEvent(RemovedEvent.of(assetFullName, key, value, replicationEvent));
            }
        }

        @Override
        public void onPut(Bytes entry, long metaDataPos, long keyPos, long valuePos,
                          boolean added, boolean replicationEvent) {
            if (subscriptions.hasSubscribers()) {
                K key = chronicleMap.readKey(entry, keyPos);
                V value = chronicleMap.readValue(entry, valuePos);
                if (added) {
                    subscriptions.notifyEvent(InsertedEvent.of(assetFullName, key, value, replicationEvent));
                } else {
                    subscriptions.notifyEvent(UpdatedEvent.of(assetFullName, key, null, value, replicationEvent));
                }
            }
        }
    }

}
