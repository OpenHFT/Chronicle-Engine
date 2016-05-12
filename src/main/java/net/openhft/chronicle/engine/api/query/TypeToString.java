package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.wire.Marshallable;

/**
 * @author Rob Austin.
 */
public interface TypeToString {

    String typeToSting(Class type);

    Class<? extends Marshallable> toType(CharSequence type);


}
