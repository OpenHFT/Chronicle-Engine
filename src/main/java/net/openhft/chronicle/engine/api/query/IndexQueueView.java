package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface IndexQueueView<S extends Subscriber<IndexedValue<V>>, V extends Marshallable>
        extends Closeable {

    void registerSubscriber(@NotNull S sub,
                            @NotNull IndexQuery<V> vanillaIndexQuery);

    void unregisterSubscriber(@NotNull S listener);

}
