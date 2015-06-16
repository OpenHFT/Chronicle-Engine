package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.ISubscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;

/**
 * Created by peter on 29/05/15.
 */
public interface KVSSubscription<K, MV, V> extends Subscription<MapEvent<K, V>>, ISubscriber, EventConsumer<K, V> {

    void registerKeySubscriber(RequestContext rc, Subscriber<K> kSubscriber);

    void unregisterKeySubscriber(Subscriber<K> kSubscriber);

    void registerTopicSubscriber(RequestContext rc, TopicSubscriber<K, V> subscriber);

    void unregisterTopicSubscriber(TopicSubscriber subscriber);

    void registerDownstream(EventConsumer<K, V> subscription);

    default boolean keyedView() {
        return true;
    }

    boolean needsPrevious();

    void setKvStore(KeyValueStore<K, MV, V> store);

    void notifyEvent(MapEvent<K, V> changeEvent);

    int keySubscriberCount();

    int entrySubscriberCount();

    int topicSubscriberCount();
}
