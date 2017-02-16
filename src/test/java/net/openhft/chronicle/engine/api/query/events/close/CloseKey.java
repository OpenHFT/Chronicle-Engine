package net.openhft.chronicle.engine.api.query.events.close;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.KeyedMarshallable;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.wire.WireMarshaller.WIRE_MARSHALLER_CL;

/**
 * @author Rob Austin.
 */
public class CloseKey extends AbstractMarshallable implements KeyedMarshallable {

    @NotNull
    private String instrument;

    CloseKey(@NotNull String instrument) {
        this.instrument = instrument;
    }

    @NotNull
    public String instrument() {
        return instrument;
    }

    public void writeKey(@NotNull Bytes bytes) {
        WIRE_MARSHALLER_CL.get(CloseKey.class).writeKey(this, bytes);
    }
}
