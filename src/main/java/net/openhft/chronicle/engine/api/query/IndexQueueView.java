package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface IndexQueueView<K extends Marshallable, V extends Marshallable> extends Closeable {

    void registerSubscriber(@NotNull Subscriber<IndexedEntry<K, V>> sub,
                            @NotNull IndexQuery<V> vanillaIndexQuery);

    void unregisterSubscriber(@NotNull Subscriber<IndexedEntry<K, V>> listener);
}
