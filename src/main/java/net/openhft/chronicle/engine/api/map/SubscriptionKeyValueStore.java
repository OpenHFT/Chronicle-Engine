package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.engine.map.KVSSubscription;

/**
 * Created by peter on 22/05/15.
 */
public interface SubscriptionKeyValueStore<K, MV, V> extends KeyValueStore<K, MV, V>, View {
    KVSSubscription<K, MV, V> subscription(boolean createIfAbsent);

}
