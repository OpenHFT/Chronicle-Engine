package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.pool.ClassLookup;
import net.openhft.chronicle.engine.EngineInstance;
import net.openhft.chronicle.engine.api.query.events.EventProcessor;
import net.openhft.chronicle.engine.api.query.events.WriterGateway;
import net.openhft.chronicle.engine.api.query.events.instrumentdata.CorpBondStaticLoadEvent;
import net.openhft.chronicle.engine.api.query.events.marketdata.MarketDataEvent;
import net.openhft.chronicle.engine.api.query.mockdata.MockDataGenerator;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.engine.api.query.IndexQuery.FROM_END;
import static net.openhft.chronicle.engine.api.query.IndexQuery.FROM_START;

/**
 * @author Rob Austin.
 */
public class VanillaIndexQueueViewTest {
    private static final String URI = "/queue/main";


    static {
        ClassLookup.create().addAlias(MarketDataEvent.class);
    }

    @Test
    public void shouldFilterEventsUsingEventName() throws Exception {
        TCPRegistry.reset();
        TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        try (VanillaAssetTree tree = EngineInstance.engineMain(1, "indexView-engine.yaml")) {
            ChronicleQueue queue = null;
            try {
                assert tree != null;
                final GenericTypesToString typesToString = new GenericTypesToString(EventProcessor.class);
                tree.root().addView(TypeToString.class, typesToString);

                queue = acquireQueue(tree, URI);
                final EventProcessor eventProcessor = new WriterGateway(queue);

                // mock data contains events for onMarketDataChanged and onCorpBondStaticChange
                new MockDataGenerator(tree).createMockData(eventProcessor);
                Thread.sleep(100);
                tree.acquireAsset(URI).acquireView(IndexQueueView.class);

                final Client client = new Client(URI, new String[]{"host.port1"}, typesToString);

                BlockingQueue<CorpBondStaticLoadEvent> eventCollector = new ArrayBlockingQueue<>(1000);
                // should only receive onCorpBondStaticChange events
                client.subscribes(CorpBondStaticLoadEvent.class, "true", FROM_START, v -> eventCollector.add(v.v()));

                CorpBondStaticLoadEvent receivedEvent = eventCollector.poll(5, TimeUnit.SECONDS);
                // ensure that event is correctly populated
                Assert.assertEquals(2000f, receivedEvent.getMinimumPiece(), 0);

            } finally {
                final File file = queue.file();
                tree.close();
                if (file.isDirectory())
                    IOTools.shallowDeleteDirWithFiles((file));

            }
        }
    }

    @Test
    public void shouldReceiveEventViaSubscription() throws InterruptedException, IOException {
        TCPRegistry.reset();
        TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        try (VanillaAssetTree tree = EngineInstance.engineMain(1, "indexView-engine.yaml")) {
            ChronicleQueue queue = null;
            try {
                assert tree != null;
                final GenericTypesToString typesToString = new GenericTypesToString(EventProcessor.class);
                tree.root().addView(TypeToString.class, typesToString);

                queue = acquireQueue(tree, URI);
                final EventProcessor eventProcessor = new WriterGateway(queue);

                // The reader event processor, e.g. pricing engine, will tail the input and process events received.
                // Methods on the event processor will be invoked directly.
//            q.createTailer().methodReader(new PricingEngine());

                new MockDataGenerator(tree).createMockData(eventProcessor);
                Thread.sleep(100);
                tree.acquireAsset(URI).acquireView(IndexQueueView.class);

                /// CLIENT
                final Client client = new Client(URI, new String[]{"host.port1"}, typesToString);

                BlockingQueue<CorpBondStaticLoadEvent> eventCollector = new ArrayBlockingQueue<>(1);
                client.subscribes(CorpBondStaticLoadEvent.class, "true", FROM_END, v -> eventCollector.add(v.v()));

                CorpBondStaticLoadEvent receivedEvent = eventCollector.poll(5, TimeUnit.SECONDS);
                System.out.println("took=" + receivedEvent);
                Assert.assertEquals(2000.75, receivedEvent.getMinimumPiece(), 0);

            } finally {
                final File file = queue.file();
                tree.close();
                if (file.isDirectory())
                    IOTools.shallowDeleteDirWithFiles((file));

            }
        }
    }

    @Nullable
    private RollingChronicleQueue acquireQueue(@NotNull AssetTree assetTree, @NotNull final String uri) {
        return ((ChronicleQueueView) assetTree.acquireQueue(uri, String.class, Marshallable.class, "clusterTwo"))
                .chronicleQueue();
    }
}