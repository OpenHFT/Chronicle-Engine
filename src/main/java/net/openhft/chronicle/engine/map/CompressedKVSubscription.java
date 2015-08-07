package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Base64;

/**
 * Created by daniel on 07/08/2015.
 */
public class CompressedKVSubscription<K> implements ObjectKVSSubscription {

    private ObjectKVSSubscription underlying;
    private Subscriber subscriber;

    public CompressedKVSubscription(RequestContext requestContext, Asset asset,
                                    ObjectKVSSubscription underlying) {
        this.underlying = underlying;
        underlying.registerDownstream(new EventConsumer<K, BytesStore>() {
            @Override
            public void notifyEvent(MapEvent<K, BytesStore> changeEvent){
                MapEvent m = changeEvent.translate(k->k, v->decompressBytesStore(v));
                System.out.println("In notify event " + m);

                if(subscriber != null) {
                    try {subscriber.onMessage(m);
                    } catch (InvalidSubscriberException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    @Override
    public void registerKeySubscriber(RequestContext rc, Subscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterTopicSubscriber(TopicSubscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerDownstream(EventConsumer subscription) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean needsPrevious() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void setKvStore(KeyValueStore store) {
        underlying.setKvStore(store);
    }

    @Override
    public void notifyEvent(MapEvent changeEvent) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean hasSubscribers() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public void unregisterSubscriber(@NotNull Subscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int keySubscriberCount() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int entrySubscriberCount() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public int topicSubscriberCount() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    private String decompressBytesStore(BytesStore bs){
        try {
            byte[] rbytes = new byte[(int) bs.readRemaining()];
            bs.copyTo(rbytes);

            //Is base64 encoded
            byte[] compressedBytes = Base64.getDecoder().decode(rbytes);

            return Snappy.uncompressString(compressedBytes);
        }catch(IOException e){
            e.printStackTrace();
            throw new IORuntimeException(e);
        }
    }
}
