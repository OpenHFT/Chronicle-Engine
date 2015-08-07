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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by daniel on 07/08/2015.
 */
//todo implement the rest of the functionality from VanillaKVSSubscription.
public class CompressedKVSubscription<K> implements ObjectKVSSubscription {

    private Asset asset;
    private KeyValueStore<K, BytesStore> kvStore;
    private boolean hasSubscribers = false;
    private ObjectKVSSubscription underlying;
    private final Set<TopicSubscriber<K, BytesStore>> topicSubscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<MapEvent<K, BytesStore>>> subscribers = new CopyOnWriteArraySet<>();
    private final Set<Subscriber<K>> keySubscribers = new CopyOnWriteArraySet<>();
    private final Set<EventConsumer<K, BytesStore>> downstream = new CopyOnWriteArraySet<>();

    public CompressedKVSubscription(RequestContext requestContext, Asset asset,
                                    ObjectKVSSubscription underlying) {
        this.asset = asset;
        this.underlying = underlying;
        underlying.registerDownstream(new EventConsumer<K, BytesStore>() {
            @Override
            public void notifyEvent(MapEvent<K, BytesStore> changeEvent){
                MapEvent uncompressedEvent = changeEvent.translate(k->k, v->decompressBytesStore(v));
                System.out.println("In notify event " + uncompressedEvent);

                if(!subscribers.isEmpty()) {
                    subscribers.forEach(s -> {
                        try {
                            s.onMessage(uncompressedEvent);
                        } catch (InvalidSubscriberException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
                }
                if(!keySubscribers.isEmpty()){
                    keySubscribers.forEach(s -> {
                        try {
                            s.onMessage((K)uncompressedEvent.key());
                        } catch (InvalidSubscriberException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
                }
                if(!topicSubscribers.isEmpty()){
                    topicSubscribers.forEach(s -> {
                        try {
                            s.onMessage((K)uncompressedEvent.key(), (BytesStore)uncompressedEvent.value());
                        } catch (InvalidSubscriberException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
                }
                if(!downstream.isEmpty()){
                    downstream.forEach(d -> {
                        try {
                            d.notifyEvent(uncompressedEvent);
                        } catch (InvalidSubscriberException e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    });
                }
            }

            @Override
            public void onEndOfSubscription(){
                subscribers.forEach(s->onEndOfSubscription());
            }
        });
    }

    @Override
    public void registerKeySubscriber(RequestContext rc, Subscriber subscriber) {
        Boolean bootstrap = rc.bootstrap();

        keySubscribers.add(subscriber);
        if (bootstrap != Boolean.FALSE && kvStore != null) {
            try {
                for (int i = 0; i < kvStore.segments(); i++)
                    kvStore.keysFor(i, subscriber::onMessage);
            } catch (InvalidSubscriberException e) {
                keySubscribers.remove(subscriber);
            }
        }
        hasSubscribers = true;
    }

    @Override
    public void registerTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        Boolean bootstrap = rc.bootstrap();
        topicSubscribers.add((TopicSubscriber<K, BytesStore>) subscriber);
        if (bootstrap != Boolean.FALSE && kvStore != null) {
            try {
                for (int i = 0; i < kvStore.segments(); i++)
                    kvStore.entriesFor(i, e -> subscriber.onMessage(e.key(), e.value()));
            } catch (InvalidSubscriberException dontAdd) {
                topicSubscribers.remove(subscriber);
            }
        }
        hasSubscribers = true;
    }




    @Override
    public boolean needsPrevious() {
        return underlying.needsPrevious();
    }

    @Override
    public void setKvStore(KeyValueStore store) {
        underlying.setKvStore(store);
        kvStore = store;
    }

    @Override
    public void notifyEvent(MapEvent changeEvent) {
        underlying.notifyEvent(changeEvent);
    }

    @Override
    public boolean hasSubscribers() {
        return hasSubscribers || asset.hasChildren();
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber subscriber) {
        Boolean bootstrap = rc.bootstrap();
        Class eClass = rc.type();
        if (eClass == KeyValueStore.Entry.class || eClass == MapEvent.class) {
            subscribers.add(subscriber);
            if (bootstrap != Boolean.FALSE && kvStore != null) {
                Subscriber<MapEvent<K, BytesStore>> sub = (Subscriber<MapEvent<K, BytesStore>>) subscriber;
                try {
                    for (int i = 0; i < kvStore.segments(); i++)
                        kvStore.entriesFor(i, sub::onMessage);
                } catch (InvalidSubscriberException e) {
                    subscribers.remove(subscriber);
                }
            }
        } else
            registerKeySubscriber(rc, subscriber);

        hasSubscribers = true;

    }

    public void unregisterDownstream(EventConsumer<K, BytesStore> subscription) {
        downstream.remove(subscription);
        updateHasSubscribers();
    }

    @Override
    public void unregisterSubscriber(@NotNull Subscriber subscriber) {
        subscribers.remove(subscriber);
        keySubscribers.remove(subscriber);
        updateHasSubscribers();
        subscriber.onEndOfSubscription();
    }

    @Override
    public void unregisterTopicSubscriber(@NotNull TopicSubscriber subscriber) {
        topicSubscribers.remove(subscriber);
        updateHasSubscribers();
        subscriber.onEndOfSubscription();
    }

    @Override
    public void registerDownstream(EventConsumer subscription) {
        downstream.add(subscription);
        hasSubscribers = true;
    }

    private void updateHasSubscribers() {
        hasSubscribers = !topicSubscribers.isEmpty() && !subscribers.isEmpty()
                && !keySubscribers.isEmpty() && !downstream.isEmpty();
    }


    @Override
    public int keySubscriberCount() {
        return keySubscribers.size();
    }

    @Override
    public int entrySubscriberCount() {
        return subscribers.size();
    }

    @Override
    public int topicSubscriberCount() {
        return topicSubscribers.size();
    }


    @Override
    public void close() {
        //todo Is there a problem with the underlying (RemoteKVSSubscription)
        //in that it doesn't send out events onEndOfSubscription
        underlying.close();
        subscribers.forEach(s->s.onEndOfSubscription());
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
