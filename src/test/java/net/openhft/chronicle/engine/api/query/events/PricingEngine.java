package net.openhft.chronicle.engine.api.query.events;

import net.openhft.chronicle.engine.api.query.events.close.ClosePriceEvent;
import net.openhft.chronicle.engine.api.query.events.instrumentdata.CorpBondStaticLoadEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.MarketDataEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.UserPricingEvent;
import net.openhft.chronicle.engine.api.query.events.user.BenchmarkChangeUserEvent;
import net.openhft.chronicle.engine.api.query.events.user.SpreadChangeUserEvent;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rob Austin.
 */
public class PricingEngine implements EventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PricingEngine.class);

    public PricingEngine() {
    }

    @Override
    public void onMarketDataChanged(@NotNull MarketDataEvent  marketData) {
        LOG.info("onMarketDataChanged : " + marketData);
    }

    @Override
    public void onUserPricingChanged(@NotNull UserPricingEvent userPricingEvent) {
        LOG.info("onMarketDataChanged : " + userPricingEvent);
    }

    @Override
    public void onBenchmarkChangeUserEvent(@NotNull BenchmarkChangeUserEvent userChange) {
        LOG.info("onBenchmarkChangeUserEvent : " + userChange);
    }

    @Override
    public void onSpreadChangeUserEvent(@NotNull SpreadChangeUserEvent userChange) {
        LOG.info("onSpreadChangeUserEvent : " + userChange);
    }


    @Override
    public void onClosePrice(@NotNull ClosePriceEvent closePriceEvent) {
        LOG.info("closePriceEvent : " + closePriceEvent);
    }

    @Override
    public void onCorpBondStaticChange(@NotNull CorpBondStaticLoadEvent corpBondStaticLoadEvent) {
        LOG.info("closePriceEvent : " + corpBondStaticLoadEvent);
    }
}
