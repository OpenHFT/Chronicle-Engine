package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.wire.Marshallable;

import java.util.function.Predicate;

/**
 * @author Rob Austin.
 */
public interface IndexQuery<V> extends Marshallable {
    long from();

    Predicate<V> filter();

    String eventName();
}
