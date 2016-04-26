package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.wire.Marshallable;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public interface ValueToKey<K extends Marshallable, V extends Marshallable> extends Function<V, K> {
}
