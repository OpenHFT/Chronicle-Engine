package net.openhft.chronicle.engine.api.map;

import net.openhft.chronicle.engine.api.tree.View;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 09/06/15.
 */
@FunctionalInterface
public interface ValueReader<U, T> extends View {
    ValueReader PASS = (u, t) -> u;

    @NotNull
    T readFrom(U underlying, T usingValue);
}
