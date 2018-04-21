package net.openhft.chronicle.engine.api.query.events.marketdata;

import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class MarketDataEvent extends MarketDataKey implements Marshallable {

    private double bidPrice;
    private double askPrice;

    public MarketDataEvent(@NotNull String instrument,
                           @NotNull String source,
                           double bidPrice,
                           double askPrice) {
        super(instrument, source);
        this.bidPrice = bidPrice;
        this.askPrice = askPrice;
    }

    public double bidPrice() {
        return bidPrice;
    }

    public double askPrice() {
        return askPrice;
    }

}
