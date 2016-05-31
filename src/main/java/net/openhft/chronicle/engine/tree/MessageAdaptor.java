package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.function.BiConsumer;

/**
 * @author Rob Austin.
 */
public interface MessageAdaptor extends BiConsumer<WireIn, WireOut>, Demarshallable, WriteMarshallable {

    default void onInitialize(Asset asset) {

    }

    @Override
    default void writeMarshallable(@NotNull WireOut wire) {

    }
}
