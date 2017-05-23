package net.openhft.chronicle.engine.api.query.mockdata;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.Timer;
import net.openhft.chronicle.engine.api.query.events.EventProcessor;
import net.openhft.chronicle.engine.api.query.events.instrumentdata.CorpBondStaticLoadEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.MarketDataEvent;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class MockDataGenerator {

    @NotNull
    private final Timer t;

    public MockDataGenerator(@NotNull AssetTree tree) {
        final EventLoop eventLoop = tree.root().acquireView(EventLoop.class);
        t = new Timer(eventLoop);
    }

    public void createMockData(@NotNull EventProcessor ep) {
        ep.onMarketDataChanged(new MarketDataEvent("VOD.L", "REUTERS", -1, 216.25));
        ep.onMarketDataChanged(new MarketDataEvent("BT.L", "REUTERS", 1, 363.60));
        ep.onMarketDataChanged(new MarketDataEvent("VOD.L", "REUTERS", -2, 217.25));
        ep.onMarketDataChanged(new MarketDataEvent("BT.L", "REUTERS", 2, 364.60));
        ep.onMarketDataChanged(new MarketDataEvent("VOD.L", "REUTERS", -3, 217.25));
        ep.onMarketDataChanged(new MarketDataEvent("BT.L", "REUTERS", 3, 364.70));
        ep.onMarketDataChanged(new MarketDataEvent("BT.L", "REUTERS", 3, 364.0));
        ep.onCorpBondStaticChange(new CorpBondStaticLoadEvent(99, 2000f)); // java.lang.UnsupportedOperationException: UINT16
        ep.onCorpBondStaticChange(new CorpBondStaticLoadEvent(99, 2000.0f)); // java.lang.UnsupportedOperationException: UINT16
        ep.onCorpBondStaticChange(new CorpBondStaticLoadEvent(99, 2000.75f)); // ok
    }

}
