package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.View;

/**
 * Created by peter on 09/06/15.
 */
@FunctionalInterface
public interface ValueReader<U, T> extends View {
    ValueReader PASS = (u, t) -> u;

    T readFrom(U underlying, T usingValue);
}
