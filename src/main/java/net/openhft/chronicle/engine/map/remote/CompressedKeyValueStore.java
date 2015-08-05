package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.EventConsumer;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.map.RawKVSSubscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by daniel on 28/07/2015.
 */
public class CompressedKeyValueStore<K> implements ObjectKeyValueStore<K, String> {

    private static final Logger LOG = LoggerFactory.getLogger(CompressedKeyValueStore.class);

    private final Class kClass;
    private Asset asset;
    private KeyValueStore<K, BytesStore> underlyingKVStore;

    @NotNull
    private RawKVSSubscription<K, String> subscriptions;

    public CompressedKeyValueStore(RequestContext context, Asset asset,
                                   KeyValueStore<K, BytesStore> kvStore) {
        this.asset = asset;
        this.underlyingKVStore = kvStore;
        this.kClass = context.keyType();

        subscriptions = asset.acquireView(RawKVSSubscription.class, context);
        subscriptions.setKvStore(this);

        SubscriptionKeyValueStore<K, BytesStore> sKVStore = (SubscriptionKeyValueStore)kvStore;

        sKVStore.subscription(true).registerDownstream(new EventConsumer<K, BytesStore>() {
            @Override
            public void notifyEvent(MapEvent<K, BytesStore> changeEvent) throws InvalidSubscriberException {
                //MapEvent m = changeEvent.translate(k->k, v->decompressBytesStore(v));
                LOG.info("In notify event");

                //subscriptions.notifyEvent(changeEvent.translate(k->k, v->decompressBytesStore(v)));
            }
        });
    }

    @Override
    public Class<K> keyType() {
        return kClass;
    }

    @Override
    public Class<String> valueType() {
        return String.class;
    }

    @Override
    public KVSSubscription<K, String> subscription(boolean createIfAbsent) {
        //Make this an ObjectSubscription see RemoteKVSubscription
        //this should subscribe to the other one
        //then call translate to convert BytesStore to String
        //then call notify
        return subscriptions;
    }

    @Nullable
    @Override
    public String getAndPut(K key, String value) {
        BytesStore ret = underlyingKVStore.getAndPut(key, compressString(value));
        return ret == null ? null : decompressBytesStore(ret);
    }

    @Nullable
    @Override
    public String getAndRemove(K key) {
        BytesStore ret = underlyingKVStore.getAndRemove(key);
        return ret == null ? null : decompressBytesStore(ret);
    }

    @Nullable
    @Override
    public String getUsing(K key, Object value) {
        Bytes compressedValue = value == null ? null : (Bytes) compressString((String)value);
        BytesStore ret = underlyingKVStore.getUsing(key, compressedValue);
        return ret == null ? null : decompressBytesStore(ret);
    }

    @Override
    public long longSize() {
        return underlyingKVStore.longSize();
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        underlyingKVStore.keysFor(segment, kConsumer);
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapEvent<K, String>> kvConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        underlyingKVStore.clear();
    }

    @Override
    public boolean containsValue(String value) {
        return underlyingKVStore.containsValue(compressString(value));
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Nullable
    @Override
    public KeyValueStore underlying() {
        return underlyingKVStore;
    }

    @Override
    public void close() {
        underlyingKVStore.close();
    }

    @Override
    public void accept(EngineReplication.ReplicationEntry replicationEntry) {
        underlyingKVStore.accept(replicationEntry);
    }

    @NotNull
    private String decompressBytesStore(BytesStore bs){
        try {
            byte[] rbytes = new byte[(int) bs.readRemaining()];
            bs.copyTo(rbytes);

            return Snappy.uncompressString(rbytes);
        }catch(IOException e){
            throw new IORuntimeException(e);
        }
    }

    @NotNull
    private BytesStore compressString(String value){
        try{
            byte[] cbytes = Snappy.compress(value);
            return Bytes.wrapForRead(cbytes);
        }catch(IOException e){
            throw new IORuntimeException(e);
        }
    }
}
