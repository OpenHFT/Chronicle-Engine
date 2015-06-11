package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.Subscription;

/**
 * Created by peter on 11/06/15.
 */
public interface TopologySubscription extends Subscription<TopologicalEvent> {
    void notifyEvent(TopologicalEvent event);
}
