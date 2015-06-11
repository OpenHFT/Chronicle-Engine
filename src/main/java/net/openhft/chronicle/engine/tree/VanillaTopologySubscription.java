package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by peter on 11/06/15.
 */
public class VanillaTopologySubscription implements TopologySubscription {
    private final Asset asset;
    private final Set<Subscriber<TopologicalEvent>> subscribers = new CopyOnWriteArraySet<>();

    public VanillaTopologySubscription(RequestContext requestContext, Asset asset) {
        this.asset = asset;
    }

    @Override
    public void registerSubscriber(RequestContext rc, Subscriber<TopologicalEvent> subscriber) {
        try {
            if (rc.bootstrap() != Boolean.FALSE)
                asset.forEachChild(c ->
                        subscriber.onMessage(ExistingAssetEvent.of(asset.fullName(), c.name())));
            subscribers.add(subscriber);
        } catch (InvalidSubscriberException e) {
            // ignored
        }
    }

    @Override
    public void unregisterSubscriber(Subscriber<TopologicalEvent> subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public void notifyEvent(TopologicalEvent event) {
        for (Subscriber<TopologicalEvent> sub : subscribers) {
            try {
                sub.onMessage(event);
            } catch (InvalidSubscriberException expected) {
                subscribers.remove(sub);
            }
        }
    }
}
