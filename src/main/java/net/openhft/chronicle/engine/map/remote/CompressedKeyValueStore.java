package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionConsumer;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import org.jetbrains.annotations.Nullable;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * Created by daniel on 28/07/2015.
 */
public class CompressedKeyValueStore<K> implements ObjectKeyValueStore<K,String, String> {
    private KeyValueStore<K, Bytes, BytesStore> remoteKeyValueStore;

    public CompressedKeyValueStore(RequestContext requestContext, Asset asset) {
        remoteKeyValueStore = new RemoteKeyValueStore(requestContext, asset, asset.findView(TcpChannelHub.class));
    }

    public CompressedKeyValueStore(RequestContext requestContext, Asset asset,
                                   KeyValueStore<K, Bytes, BytesStore> kvStore) {
        this.remoteKeyValueStore = kvStore;
    }

    @Override
    public Class<K> keyType() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Class<String> valueType() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public KVSSubscription<K, String, String> subscription(boolean createIfAbsent) {
        //Make this an ObjectSubscription see RemoteKVSubscription
        //this should subscribe to the other one
        //then call translate to convert BytesStore to String
        //then call notify
        throw new UnsupportedOperationException("todo");
    }

    @Nullable
    @Override
    public String getAndPut(K key, String value) {
            //Compress V
            System.out.println("Value " + value);

            try {
                byte[] cbytes = Snappy.compress(value);

                BytesStore bs = Bytes.wrapForRead(cbytes);
                BytesStore ret = remoteKeyValueStore.getAndPut(key, bs);
                if(ret==null)return null;
                byte[] rbytes = new byte[(int)ret.readRemaining()];
                ret.copyTo(rbytes);
                System.out.println("c->" + Bytes.wrapForRead(cbytes).toHexString());
                System.out.println("r->" + Bytes.wrapForRead(rbytes).toHexString());
                String r =  Snappy.uncompressString(rbytes);
                System.out.println("**" + r);
                return r;



            } catch (IOException e) {
                throw new AssertionError(e);
            }

//        try {
//
//
//        } catch (IOException e) {
//            throw new AssertionError(e);
//        }
    }

    @Nullable
    @Override
    public String getAndRemove(K key) {
        throw new UnsupportedOperationException("todo");
    }

    @Nullable
    @Override
    public String getUsing(K key, String value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public long longSize() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void keysFor(int segment, SubscriptionConsumer<K> kConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void entriesFor(int segment, SubscriptionConsumer<MapEvent<K, String>> kvConsumer) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean containsValue(String value) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Asset asset() {
        throw new UnsupportedOperationException("todo");
    }

    @Nullable
    @Override
    public KeyValueStore<K, String, String> underlying() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void accept(EngineReplication.ReplicationEntry replicationEntry) {
        throw new UnsupportedOperationException("todo");
    }
}
