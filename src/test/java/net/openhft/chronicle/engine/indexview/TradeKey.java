package net.openhft.chronicle.engine.indexview;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.KeyedMarshallable;
import net.openhft.chronicle.wire.WireMarshaller;
import org.jetbrains.annotations.NotNull;

public class TradeKey extends AbstractMarshallable implements KeyedMarshallable {
    private String tradeId;

    String getTradeId() {
        return tradeId;
    }

    void setTradeId(String tradeId) {
        this.tradeId = tradeId;
    }

    public void writeKey(@NotNull Bytes bytes) {
        WireMarshaller.WIRE_MARSHALLER_CL.get(TradeKey.class).writeKey(this, bytes);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tradeId == null) ? 0 : tradeId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TradeKey other = (TradeKey) obj;
        if (tradeId == null) {
            if (other.tradeId != null)
                return false;
        } else if (!tradeId.equals(other.tradeId))
            return false;
        return true;
    }

}