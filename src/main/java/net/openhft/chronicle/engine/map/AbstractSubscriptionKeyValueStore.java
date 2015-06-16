package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

/**
 * Created by daniel on 08/06/15.
 */
public class AbstractSubscriptionKeyValueStore<K, MV,V> extends AbstractKeyValueStore<K, MV,V>
        implements SubscriptionKeyValueStore<K, MV,V> {
    protected AbstractSubscriptionKeyValueStore(RequestContext rc, Asset asset, @NotNull SubscriptionKeyValueStore<K, MV, V> kvStore) {
        super(rc, asset, kvStore);
    }

    @Override
    public KVSSubscription subscription(boolean createIfAbsent) {
        return ((SubscriptionKeyValueStore<K, MV,V>) kvStore).subscription(createIfAbsent);
    }
}
