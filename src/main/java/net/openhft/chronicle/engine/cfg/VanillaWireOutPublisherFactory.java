package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.network.MarshallableFunction;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class VanillaWireOutPublisherFactory implements MarshallableFunction<WireType,
        WireOutPublisher>, Demarshallable {

    private VanillaWireOutPublisherFactory(WireIn wireIn) {
    }

    public VanillaWireOutPublisherFactory() {
    }

    @Override
    public WireOutPublisher apply(WireType wireType) {
        return new VanillaWireOutPublisher(wireType);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

    }
}
