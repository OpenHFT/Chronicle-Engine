package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.hash.replication.EngineReplicationLangBytesConsumer;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.MapEventListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer.notifyEachEvent;
import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;
import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;


public class ChronicleMapKeyValueStore<K, MV, V> implements SubscriptionKeyValueStore<K, MV, V>, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapKeyValueStore.class);
    private final ChronicleMap<K, V> chronicleMap;
    private final ObjectKVSSubscription<K, MV, V> subscriptions;
    private final EngineReplication engineReplicator;
    private Asset asset;

    public ChronicleMapKeyValueStore(@NotNull RequestContext context, Asset asset) {
        this.asset = asset;
        PublishingOperations publishingOperations = new PublishingOperations();

        Class kClass = context.type();
        Class vClass = context.type2();
        String basePath = context.basePath();


        ChronicleMapBuilder<K, V> builder = ChronicleMapBuilder.<K, V>of(kClass, vClass);
        EngineReplication engineReplicator = null;
        try {
            engineReplicator = asset.acquireView(requestContext(context.name()).viewType
                    (EngineReplication.class));

            EngineReplicationLangBytesConsumer langBytesConsumer = asset.acquireView(requestContext(context.name())
                    .viewType
                            (EngineReplicationLangBytesConsumer.class));

            
            HostIdentifier hostIdentifier = asset.acquireView(HostIdentifier.class, context);

            builder.replication(builder().engineReplication( langBytesConsumer)
                    .createWithId((byte) hostIdentifier.hostId()));
        } catch (AssetNotFoundException anfe) {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("replication not enabled " + anfe.getMessage());
        }
        this.engineReplicator = engineReplicator;
        builder.eventListener(publishingOperations);


        if (context.putReturnsNull() != Boolean.FALSE) {
            builder.putReturnsNull(true);
        }
        if (context.getAverageValueSize() != 0) {
            builder.averageValueSize(context.getAverageValueSize());
        }
        if (context.getEntries() != 0) {
            builder.entries(context.getEntries());
        }
        if (basePath == null)
            builder.create();
        else {
            String pathname = basePath + "/" + context.name();
            new File(basePath).mkdirs();
            try {
                builder.createPersistedTo(new File(pathname));
            } catch (IOException e) {
                IORuntimeException iore = new IORuntimeException("Could not access " + pathname);
                iore.initCause(e);
                throw iore;
            }
        }

        chronicleMap = builder.create();
        subscriptions = asset.acquireView(ObjectKVSSubscription.class, context);
        subscriptions.setKvStore(this);
    }

 
    @NotNull
    @Override
    public KVSSubscription<K, MV, V> subscription(boolean createIfAbsent) {
        return subscriptions;
    }

    @Override
    public V getAndPut(K key, V value) {
        return chronicleMap.put(key, value);
    }

    @Override
    public V getAndRemove(K key) {
        return chronicleMap.remove(key);
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
        chronicleMap.entrySet().stream().map(e -> InsertedEvent.of(asset.fullName(), e.getKey(), e.getValue())).forEach(e -> {
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

    @Override
    public Iterator<K> keySetIterator() {
        return chronicleMap.keySet().iterator();
    }

    @Override
    public void clear() {
        chronicleMap.clear();
    }

    @Override
    public boolean containsValue(final MV value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void apply(@NotNull final ReplicationEntry entry) {
        engineReplicator.applyReplication(entry);
    }

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
        chronicleMap.close();
    }

    class PublishingOperations extends MapEventListener<K, V> {
        @Override
        public void onRemove(@NotNull K key, V value, boolean replicationEven) {
            subscriptions.notifyEvent(RemovedEvent.of(asset.fullName(), key, value));
        }

        @Override
        public void onPut(@NotNull K key, V newValue, @Nullable V replacedValue, boolean replicationEvent) {
            if (replacedValue != null) {
                subscriptions.notifyEvent(UpdatedEvent.of(asset.fullName(), key, replacedValue, newValue));
            } else {
                subscriptions.notifyEvent(InsertedEvent.of(asset.fullName(), key, newValue));
            }
        }
    }

}
