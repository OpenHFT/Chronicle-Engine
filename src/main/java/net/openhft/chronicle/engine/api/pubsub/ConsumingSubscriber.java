package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;

/**
 * @author Rob Austin.
 */
public interface ConsumingSubscriber<E> extends Subscriber<E>, Closeable {
    void addWireConsumer(VanillaWireOutPublisher.WireOutConsumer wireOutConsumer);
}
