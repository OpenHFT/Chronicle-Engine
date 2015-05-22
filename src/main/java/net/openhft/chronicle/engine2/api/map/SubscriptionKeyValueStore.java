package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Subscription;

/**
 * Created by peter on 22/05/15.
 */
public interface SubscriptionKeyValueStore<K, V> extends Subscription, KeyValueStore<K, V> {
}
