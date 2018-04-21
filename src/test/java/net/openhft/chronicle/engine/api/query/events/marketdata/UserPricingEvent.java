package net.openhft.chronicle.engine.api.query.events.marketdata;

import net.openhft.chronicle.wire.Marshallable;

public class UserPricingEvent extends UserPricingKey implements Marshallable {

    private double bidQty;
    private double askQty;

    public UserPricingEvent(int uiid,
                            double bidQty,
                            double askQty) {
        super(uiid);
        this.bidQty = bidQty;
        this.askQty = askQty;
    }

    public double bidQty() {
        return bidQty;
    }

    public double askQty() {
        return askQty;
    }
}
