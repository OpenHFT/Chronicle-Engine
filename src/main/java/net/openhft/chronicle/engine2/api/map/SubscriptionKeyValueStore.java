package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.View;
import net.openhft.chronicle.engine2.map.SubscriptionKVSCollection;

/**
 * Created by peter on 22/05/15.
 */
public interface SubscriptionKeyValueStore<K, MV, V> extends KeyValueStore<K, MV, V>, View {
    SubscriptionKVSCollection<K, V> subscription(boolean createIfAbsent);

}
