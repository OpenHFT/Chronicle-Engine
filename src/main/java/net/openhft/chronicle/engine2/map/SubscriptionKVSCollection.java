package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Subscription;

/**
 * Created by peter on 29/05/15.
 */
public interface SubscriptionKVSCollection<K, V> extends Subscription {
    void notifyUpdate(K key, V oldValue, V value);

    void notifyRemoval(K key, V oldValue);

    default boolean keyedView() {
        return true;
    }
}
