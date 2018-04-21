package net.openhft.chronicle.engine.indexview;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.EngineInstance;
import net.openhft.chronicle.engine.ShutdownHooks;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.*;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIndexQueueView {

    private static final int COUNT = 10_000;
    private static final String TRADES = "TRADES";

    private static final Random random = new Random();
    private static final String[] isins = {"GB0008931148", "GB00B1VWPC84", "GB00B39R3F84", "GB00B4YRFP41",
            "GB00B058DQ55"};
    private static final String[] books = {"CASH_BOOK1", "CASH_BOOK2", "CASH_BOOK3", "REPO_BOOK1", "REPO_BOOK2"};
    private static final String[] cptys = {"x", "y", "z", "a", "b"};
    private static final String TRADES_Q = "tradesQ";

    @Rule
    public ShutdownHooks hooks = new ShutdownHooks();

    /**
     * adds some test data to a queue
     *
     * @return returns all the trade ID's that where used
     */
    private static Set<String> publishMockData() {
        Set<String> tradeIds = new LinkedHashSet<>();

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(TRADES_Q).build()) {

            ExcerptAppender excerptAppender = queue.acquireAppender();
            MockTradeGenerator mockTrade = new MockTradeGenerator();

            for (int i = 0; i < COUNT; i++) {

                try (DocumentContext dc = excerptAppender.writingDocument()) {
                    Trade tradeId = mockTrade.apply(i);
                    dc.wire().write(TRADES).marshallable(tradeId);
                    tradeIds.add(tradeId.getTradeId());
                }
            }
            return tradeIds;
        }
    }

    static void deleteDir(@NotNull File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDir(file);
                    } else if (!file.delete()) {
                        System.out.println("... unable to delete {}" + file);
                    }
                }
            }
        }

        dir.delete();
    }

    @Before
    public void setUp() {
        deleteDir(new File("tradesQ"));
    }

    @Test
    public void test() throws IOException, InterruptedException {
        //YamlLogging.setAll(true);

        TCPRegistry.createServerSocketChannelFor("host.port1");
        ClassAliasPool.CLASS_ALIASES.addAlias(Trade.class, "TRADE_M");
        String uri = "/queue/trades";

        Set<String> tradeIds = publishMockData();

        startEngine();

        VanillaAssetTree assetTree = hooks.addCloseable((new VanillaAssetTree()).forRemoteAccess("host.port1", WireType.BINARY));

        VanillaAsset asset = (VanillaAsset) assetTree.acquireAsset(uri);
        assetTree.root().getRuleProvider().configQueueRemote(asset);

        IndexQueueView indexQueueView = assetTree.acquireAsset(uri).acquireView(IndexQueueView.class);
        VanillaIndexQuery indexQuery = new VanillaIndexQuery();
        indexQuery.select(Trade.class, "true");
        indexQuery.fromIndex(IndexQuery.FROM_END);
        indexQuery.eventName(TRADES);
        Set<String> m = new HashSet<>();

        Subscriber s = subscribe(tradeIds, m);
        indexQueueView.registerSubscriber(s, indexQuery);

        // wait up to seconds for the expected result
        for (int i = 0; i < 10_000; i++) {
            Thread.sleep(1);
            if (tradeIds.isEmpty())
                break;
        }

        assertTrue("Expected empty, but was: " + tradeIds.size(), tradeIds.isEmpty());

        indexQueueView.unregisterSubscriber(s);

        tradeIds.addAll(publishMockData());
        int size = tradeIds.size();
        assertTrue(size > 0);
        Thread.sleep(2000);
        assertEquals(size, tradeIds.size());

        indexQueueView.registerSubscriber(s, indexQuery);
        for (int i = 0; i < 10_000; i++) {
            Thread.sleep(1);
            if (tradeIds.isEmpty())
                break;
        }

        assertTrue("Expected empty, but was: " + tradeIds.size(), tradeIds.isEmpty());

    }

    @NotNull
    private Subscriber subscribe(Set<String> tradeIds, Set<String> m) {
        return indexedValue -> {
            Trade v = (Trade) ((IndexedValue) indexedValue).v();
            tradeIds.remove(v.getTradeId());
            m.add(v.getTradeId());
        };
    }

    private void startEngine() {
        VanillaAssetTree serverTree =
                hooks.addCloseable(EngineInstance.engineMain(1, "indexView-engine.yaml"));

        final TypeToString typesToString = new TypeToString() {
            @Override
            public String typeToString(Class aClass) {
                return null;
            }

            @Override
            public Class<? extends Marshallable> toType(CharSequence c) {
                if (TRADES.contentEquals(c))
                    return Trade.class;
                throw new UnsupportedOperationException();
            }
        };

        serverTree.root().addView(TypeToString.class, typesToString);
    }

    private static class MockTradeGenerator implements Function<Integer, Marshallable> {

        private Trade trade = new Trade();

        @Override
        public Trade apply(Integer i) {
            trade.setTradeId("TRDID-" + i);
            trade.isin = isins[random.nextInt(isins.length)];
            trade.book = books[random.nextInt(books.length)];
            trade.quantity = 100000.0 * (random.nextInt(cptys.length) + 1);
            trade.price = 101.0;

            return trade;
        }

    }

}
