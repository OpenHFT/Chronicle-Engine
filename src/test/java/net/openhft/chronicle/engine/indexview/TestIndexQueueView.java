package net.openhft.chronicle.engine.indexview;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.EngineInstance;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.*;
import net.openhft.chronicle.engine.api.tree.AssetTree;
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
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;


public class TestIndexQueueView {

    private static final int COUNT = 100_000;
    private static final String TRADES = "TRADES";

    private static final Random random = new Random();
    private static final String[] isins = {"GB0008931148", "GB00B1VWPC84", "GB00B39R3F84", "GB00B4YRFP41",
            "GB00B058DQ55"};
    private static final String[] books = {"CASH_BOOK1", "CASH_BOOK2", "CASH_BOOK3", "REPO_BOOK1", "REPO_BOOK2"};
    private static final String[] cptys = {"x", "y", "z", "a", "b"};


    @Test
    public void test() throws IOException, InterruptedException {

        TCPRegistry.createServerSocketChannelFor(" host.port1");
        ClassAliasPool.CLASS_ALIASES.addAlias(Trade.class, "TRADE_M");
        String uri = "/queue/trades";

        TestIndexQueueView testServer = new TestIndexQueueView();
        Set<String> tradeIds = testServer.publishMockData("tradesQ");

        startEngine();

        AssetTree assetTree = (new VanillaAssetTree()).forRemoteAccess(" host.port1", WireType.BINARY);


        VanillaAsset asset = (VanillaAsset) assetTree.acquireAsset(uri);
        asset.configQueueRemote();

        IndexQueueView indexQueueView = assetTree.acquireAsset(uri).acquireView(IndexQueueView.class);
        VanillaIndexQuery indexQuery = new VanillaIndexQuery();
        indexQuery.select(Trade.class, "true");
        indexQuery.fromIndex(IndexQuery.FROM_END);
        indexQuery.eventName(TRADES);
        Set<String> m = new HashSet<>();

        indexQueueView.registerSubscriber(subscribe(tradeIds, m), indexQuery);


        // wait up to seconds for the expected result
        for (int i = 0; i < 10_000; i++) {
            Thread.sleep(1);
            if (tradeIds.isEmpty())
                break;
        }

        Assert.assertTrue(tradeIds.isEmpty());
    }

    @NotNull
    private Subscriber subscribe(Set<String> tradeIds, Set<String> m) {
        return indexedValue -> {
            Trade v = (Trade) ((IndexedValue) indexedValue).v();
            tradeIds.remove(v.getTradeId());
            m.add(v.getTradeId());
        };
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


    /**
     * adds some test data to a queue
     *
     * @param tradesQ
     * @return returns all the trade ID's that where used
     */
    private Set<String> publishMockData(final String tradesQ) {
        Set<String> tradeIds = new LinkedHashSet<>();
        deleteDir(new File(tradesQ));

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(tradesQ).build()) {

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


    private void startEngine() {
        VanillaAssetTree serverTree = EngineInstance.engineMain(1, "indexView-engine.yaml");

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

}
