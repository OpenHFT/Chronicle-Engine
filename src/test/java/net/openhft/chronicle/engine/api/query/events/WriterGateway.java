package net.openhft.chronicle.engine.api.query.events;

import net.openhft.chronicle.engine.api.query.events.close.ClosePriceEvent;
import net.openhft.chronicle.engine.api.query.events.instrumentdata.CorpBondStaticLoadEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.MarketDataEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.UserPricingEvent;
import net.openhft.chronicle.engine.api.query.events.user.BenchmarkChangeUserEvent;
import net.openhft.chronicle.engine.api.query.events.user.SpreadChangeUserEvent;
import net.openhft.chronicle.queue.ChronicleQueue;
import org.jetbrains.annotations.NotNull;

/**
 * Gateway
 */
public class WriterGateway implements EventProcessor {

    private final EventProcessor actions;

    public WriterGateway(ChronicleQueue q) {
        // This creates an interface proxy so each message called is written to a file for replay
        actions = q.acquireAppender().methodWriter(EventProcessor.class);
    }

    @Override
    public void onMarketDataChanged(@NotNull MarketDataEvent marketData) {
        actions.onMarketDataChanged(marketData);
    }

    @Override
    public void onUserPricingChanged(@NotNull UserPricingEvent userPricingEvent) {
        actions.onUserPricingChanged(userPricingEvent);
    }

    @Override
    public void onBenchmarkChangeUserEvent(@NotNull BenchmarkChangeUserEvent userChange) {
        actions.onBenchmarkChangeUserEvent(userChange);
    }

    @Override
    public void onSpreadChangeUserEvent(@NotNull SpreadChangeUserEvent userChange) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void onClosePrice(@NotNull ClosePriceEvent userChange) {
        actions.onClosePrice(userChange);
    }

    @Override
    public void onCorpBondStaticChange(@NotNull CorpBondStaticLoadEvent corpBondStaticLoadEvent) {
        actions.onCorpBondStaticChange(corpBondStaticLoadEvent);
    }
}
