package net.openhft.chronicle.engine.api.query.events.close;

import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class ClosePriceEvent extends CloseKey {
    private double close;

    public ClosePriceEvent(@NotNull String instrument, double close) {
        super(instrument);
        this.close = close;
    }

    public double close() {
        return close;
    }

}
