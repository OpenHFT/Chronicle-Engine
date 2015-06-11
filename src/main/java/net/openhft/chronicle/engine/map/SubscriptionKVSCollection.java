package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.ISubscriber;
import net.openhft.chronicle.engine.api.Subscription;
import net.openhft.chronicle.engine.api.map.ChangeEvent;
import net.openhft.chronicle.engine.api.map.KeyValueStore;

/**
 * Created by peter on 29/05/15.
 */
public interface SubscriptionKVSCollection<K, MV, V> extends Subscription<K, V>, ISubscriber, EventConsumer<K, V> {

    default boolean keyedView() {
        return true;
    }

    boolean needsPrevious();

    void setKvStore(KeyValueStore<K,MV,V> store);

    void notifyEvent(ChangeEvent<K, V> changeEvent);
}
