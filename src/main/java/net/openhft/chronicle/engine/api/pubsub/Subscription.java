package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscription<E> extends View {
    void registerSubscriber(RequestContext rc, Subscriber<E> subscriber);

    void unregisterSubscriber(Subscriber<E> subscriber);
}
