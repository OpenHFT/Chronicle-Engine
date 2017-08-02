package net.openhft.chronicle.engine.api.query.events.marketdata;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.KeyedMarshallable;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.wire.WireMarshaller.WIRE_MARSHALLER_CL;

/**
 * @author Rob Austin.
 */
class MarketDataKey extends AbstractMarshallable implements KeyedMarshallable {
    private String instrument;
    private String source;

    MarketDataKey(String instrument, String source) {
        this.instrument = instrument;
        this.source = source;
    }

    public MarketDataKey() {
    }

    public String instrument() {
        return instrument;
    }

    public String source() {
        return source;
    }

    @Override
    public void writeKey(@NotNull Bytes bytes) {
        WIRE_MARSHALLER_CL.get(MarketDataKey.class).writeKey(this, bytes);
    }

}
