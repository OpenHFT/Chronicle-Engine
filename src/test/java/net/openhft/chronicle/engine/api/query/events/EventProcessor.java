package net.openhft.chronicle.engine.api.query.events;

import net.openhft.chronicle.engine.api.query.events.close.ClosePriceEvent;
import net.openhft.chronicle.engine.api.query.events.instrumentdata.CorpBondStaticLoadEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.MarketDataEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.UserPricingEvent;
import net.openhft.chronicle.engine.api.query.events.user.BenchmarkChangeUserEvent;
import net.openhft.chronicle.engine.api.query.events.user.SpreadChangeUserEvent;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface EventProcessor {

    void onMarketDataChanged(@NotNull MarketDataEvent marketData);

    void onUserPricingChanged(@NotNull UserPricingEvent userPricingEvent);

    void onBenchmarkChangeUserEvent(@NotNull BenchmarkChangeUserEvent userChange);

    void onSpreadChangeUserEvent(@NotNull SpreadChangeUserEvent userChange);

    void onClosePrice(@NotNull ClosePriceEvent userChange);

    void onCorpBondStaticChange(@NotNull CorpBondStaticLoadEvent corpBondStaticLoadEvent);
}
