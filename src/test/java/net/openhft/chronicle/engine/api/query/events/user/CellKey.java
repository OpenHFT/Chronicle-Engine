package net.openhft.chronicle.engine.api.query.events.user;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.KeyedMarshallable;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.wire.WireMarshaller.WIRE_MARSHALLER_CL;

/**
 * @author Rob Austin.
 */

public class CellKey extends AbstractMarshallable implements KeyedMarshallable {
    @NotNull
    private String cellId; // assumed to cell in the pricing grid

    CellKey(@NotNull String cellId) {
        this.cellId = cellId;
    }

    public void writeKey(@NotNull Bytes bytes) {
        WIRE_MARSHALLER_CL.get(CellKey.class).writeKey(this, bytes);
    }

    @NotNull
    public String cellId() {
        return cellId;
    }


}
