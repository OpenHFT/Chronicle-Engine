package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.wire.ReadMarshallable;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Rob Austin.
 */
@FunctionalInterface
public interface ObjectCacheFactory extends Supplier<Function<Class, ReadMarshallable>> {
}
