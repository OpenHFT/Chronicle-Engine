package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;

/**
 * Created by daniel on 08/06/15.
 */
public class AbstractSubscriptionKeyValueStore<K, MV,V> extends AbstractKeyValueStore<K, MV,V>
        implements SubscriptionKeyValueStore<K, MV,V> {
    protected AbstractSubscriptionKeyValueStore(SubscriptionKeyValueStore<K, MV,V> kvStore) {
        super(kvStore);
    }

    @Override
    public SubscriptionKVSCollection subscription(boolean createIfAbsent) {
        return ((SubscriptionKeyValueStore<K, MV,V>) kvStore).subscription(createIfAbsent);
    }
}
