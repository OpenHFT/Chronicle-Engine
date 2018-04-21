package net.openhft.chronicle.engine.api.query.events.instrumentdata;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.KeyedMarshallable;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.wire.WireMarshaller.WIRE_MARSHALLER_CL;

public class CorpBondStaticKey extends AbstractMarshallable implements KeyedMarshallable {
    private int uiid;

    CorpBondStaticKey(int uiid) {
        this.uiid = uiid;
    }

    public int uiid() {
        return uiid;
    }

    @Override
    public void writeKey(@NotNull Bytes bytes) {
        WIRE_MARSHALLER_CL.get(CorpBondStaticKey.class).writeKey(this, bytes);
    }

}