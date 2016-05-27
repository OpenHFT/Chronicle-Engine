package net.openhft.chronicle.engine.api.pubsub;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.Marshallable;

import java.util.List;
import java.util.function.Supplier;

/**
 * @author Rob Austin.
 */
public interface ConsumingSubscriber<E> extends Subscriber<E>, Closeable {
    void addSupplier(Supplier<List<Marshallable>> supplier);
}
