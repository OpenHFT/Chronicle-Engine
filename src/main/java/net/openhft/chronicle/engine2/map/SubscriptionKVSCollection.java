package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.ISubscriber;
import net.openhft.chronicle.engine2.api.InvalidSubscriberException;
import net.openhft.chronicle.engine2.api.Subscription;

/**
 * Created by peter on 29/05/15.
 */
public interface SubscriptionKVSCollection<K, V> extends Subscription, ISubscriber {
    void notifyUpdate(K key, V oldValue, V value) throws InvalidSubscriberException;

    void notifyRemoval(K key, V oldValue) throws InvalidSubscriberException;

    default boolean keyedView() {
        return true;
    }
}
