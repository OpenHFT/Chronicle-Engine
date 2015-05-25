package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Subscription;
import net.openhft.chronicle.engine2.api.View;

/**
 * Created by peter on 22/05/15.
 */
public interface SubscriptionKeyValueStore<K, MV, V> extends Subscription, KeyValueStore<K, MV, V>, View {
}
