package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.ISubscriber;
import net.openhft.chronicle.engine.api.Subscription;

/**
 * Created by peter on 29/05/15.
 */
public interface SubscriptionKVSCollection<K, V> extends Subscription, ISubscriber, EventConsumer<K, V> {

    default boolean keyedView() {
        return true;
    }

    boolean needsPrevious();
}
