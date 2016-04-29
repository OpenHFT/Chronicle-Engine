package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.wire.ReadMarshallable;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
@FunctionalInterface
public interface ObjectCache extends Function<CharSequence, ReadMarshallable> {

}
