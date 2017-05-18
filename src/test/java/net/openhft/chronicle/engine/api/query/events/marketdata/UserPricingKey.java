package net.openhft.chronicle.engine.api.query.events.marketdata;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.KeyedMarshallable;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.wire.WireMarshaller.WIRE_MARSHALLER_CL;


class UserPricingKey extends AbstractMarshallable implements KeyedMarshallable {
    private int uiid;

    UserPricingKey(int uiid) {
        this.uiid = uiid;
    }

    public UserPricingKey() {
    }

    public int uiid() {
        return uiid;
    }

    public void writeKey(@NotNull Bytes bytes) {
        WIRE_MARSHALLER_CL.get(UserPricingKey.class).writeKey(this, bytes);
    }

}
