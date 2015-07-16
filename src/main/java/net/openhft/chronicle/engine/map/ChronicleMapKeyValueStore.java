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
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEventListener;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.api.EventLoop;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
import static net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer.notifyEachEvent;
import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;


public class ChronicleMapKeyValueStore<K, MV, V> implements AuthenticatedKeyValueStore<K, MV, V>,
        Closeable, Supplier<EngineReplication> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
    private final ChronicleMap<K, V> chronicleMap;
    @NotNull
    private final ObjectKVSSubscription<K, MV, V> subscriptions;
    @Nullable
    private final EngineReplication engineReplicator;
    @NotNull
    private final Asset asset;
    @NotNull
    private final String assetFullName;
    @Nullable
    private final EventLoop eventLoop;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    public ChronicleMapKeyValueStore(@NotNull RequestContext context, @NotNull Asset asset) {
        String basePath = context.basePath();
        double averageValueSize = context.getAverageValueSize();
        long maxEntries = context.getEntries();
        this.asset = asset;
        this.assetFullName = asset.fullName();
        this.subscriptions = asset.acquireView(ObjectKVSSubscription.class, context);
        this.subscriptions.setKvStore(this);
        this.eventLoop = asset.findOrCreateView(EventLoop.class);
        eventLoop.start();

        PublishingOperations publishingOperations = new PublishingOperations();

        ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.of(context.keyType(), context.valueType());
        HostIdentifier hostIdentifier = null;
        EngineReplication engineReplicator1 = null;
        try {
            engineReplicator1 = asset.acquireView(EngineReplication.class);

            final EngineReplicationLangBytesConsumer langBytesConsumer = asset.findView
                    (EngineReplicationLangBytesConsumer.class);

            hostIdentifier = asset.findOrCreateView(HostIdentifier.class);

            builder.replication(builder().engineReplication(langBytesConsumer)
                    .createWithId(hostIdentifier.hostId()));

        } catch (AssetNotFoundException anfe) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("replication not enabled " + anfe.getMessage());
        }

        this.engineReplicator = engineReplicator1;
        builder.eventListener(publishingOperations);

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
                LOGGER.warn("no clusters found.");
                return;
            }

            Cluster cluster = clusters.get(context.cluster());

            if (clusters == null) {
                LOGGER.warn("no cluster found.");
                return;
            }

            byte localIdentifier = hostIdentifier.hostId();

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("hostDetails : localIdentifier=" + localIdentifier + ",cluster=" + cluster.hostDetails());

            for (HostDetails hostDetails : cluster.hostDetails()) {


                // its the identifier with the larger values that will establish the connection
                int remoteIdentifier = hostDetails.hostId;


                if (remoteIdentifier <= localIdentifier) {
                    // if (LOGGER.isDebugEnabled())
                    System.out.println("skipping : attempting to connect to localIdentifier=" + localIdentifier + ",remoteIdentifier=" + remoteIdentifier);
                    continue;
                }

                //if (LOGGER.isDebugEnabled())
                System.out.println("attempting to connect to localIdentifier=" + localIdentifier + ",remoteIdentifier=" + remoteIdentifier);

                final TcpChannelHub tcpChannelHub = hostDetails.acquireTcpChannelHub(asset, eventLoop, context.wireType());
                ReplicationHub replicationHub = new ReplicationHub(context, tcpChannelHub, eventLoop, isClosed);

                try {
                    replicationHub.bootstrap(engineReplicator1, localIdentifier, (byte) remoteIdentifier);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            System.out.println("loop exit");
        }

    }

    @NotNull
    @Override
    public KVSSubscription<K, MV, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Nullable
    @Override
    public V getAndPut(K key, V value) {
        if (!isClosed.get())
            return chronicleMap.put(key, value);
        else
            return null;

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
    public V getUsing(K key, @Nullable MV value) {
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
        chronicleMap.entrySet().stream().map(e -> InsertedEvent.of(assetFullName, e.getKey(), e.getValue())).forEach(e -> {
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
    public KeyValueStore<K, MV, V> underlying() {
        return null;
    }

    @Override
    public void close() {
        isClosed.set(true);
        eventLoop.stop();
        closeQuietly(asset.findView(TcpChannelHub.class));
        new Thread(() -> {
            Jvm.pause(1000);
            chronicleMap.close();
        }, "close " + assetFullName).start();
    }

    @Override
    public void accept(@NotNull final ReplicationEntry replicationEntry) {
        if (!isClosed.get())
            engineReplicator.applyReplication(replicationEntry);
        else
            LOGGER.warn("message skipped as closed replicationEntry=" + replicationEntry);
    }

    @Nullable
    @Override
    public EngineReplication get() {
        return engineReplicator;
    }

    class PublishingOperations extends MapEventListener<K, V> {
        @Override
        public void onRemove(@NotNull K key, V value, boolean replicationEven) {
            if (subscriptions.hasSubscribers())
                subscriptions.notifyEvent(RemovedEvent.of(assetFullName, key, value));
        }

        @Override
        public void onPut(@NotNull K key, V newValue, @Nullable V replacedValue, boolean replicationEvent) {
            if (subscriptions.hasSubscribers())
                if (replacedValue == null) {
                    subscriptions.notifyEvent(InsertedEvent.of(assetFullName, key, newValue));
                } else {
                    subscriptions.notifyEvent(UpdatedEvent.of(assetFullName, key, replacedValue, newValue));
                }
        }
    }

}
