package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;

/**
 * Created by peter on 22/05/15.
 */
public interface Publisher<E> {
    void publish(E event);

    void registerSubscriber(Subscriber<E> subscriber) throws AssetNotFoundException;
}
