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
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.engine.api.query.IndexQuery.FROM_END;

/**
 * @author Rob Austin.
 */
public class VanillaIndexQueueViewTest {

    private static final String URI = "/queue/main";
    public static final String VOD_L = "\"VOD.L\"";
    public static final String BT_L = "\"BT.L\"";


    static {
        ClassLookup.create().addAlias(MarketDataEvent.class);
    }

    private RollingChronicleQueue queue(@NotNull AssetTree assetTree, final String uri) {
        return ((ChronicleQueueView) assetTree.acquireQueue(uri, String.class, Marshallable.class, "clusterTwo"))
                .chronicleQueue();
    }

    @Test
    public void test() throws InterruptedException, IOException {
        TCPRegistry.reset();
        TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        final VanillaAssetTree tree = EngineInstance.engineMain(1, "indexView-engine.yaml");
        ChronicleQueue q = null;
        try {
            assert tree != null;
            final GenericTypesToString typesToString = new GenericTypesToString(EventProcessor.class);
            tree.root().addView(TypeToString.class, typesToString);

            q = queue(tree, URI);
            final EventProcessor eventProcessor = new WriterGateway(q);

            // The reader event processor, e.g. pricing engine, will tail the input and process events received.
            // Methods on the event processor will be invoked directly.
//            q.createTailer().methodReader(new PricingEngine());

            new MockDataGenerator(tree).createMockData(eventProcessor);
            Thread.sleep(100);
            tree.acquireAsset(URI).acquireView(IndexQueueView.class);


            /// CLIENT
            final Client client = new Client(URI, new String[]{"host.port1"}, typesToString);

            ArrayBlockingQueue q1 = new ArrayBlockingQueue(1);
            client.subscribes(CorpBondStaticLoadEvent.class,
                    "true", FROM_END,
                    v -> q1.add(v.v()));

            CorpBondStaticLoadEvent take = (CorpBondStaticLoadEvent) q1.poll(5, TimeUnit.SECONDS);
            System.out.println("took=" + take);
            Assert.assertEquals(2000.75, take.getMinimumPiece(), 0);

        } finally {
            final File file = q.file();
            System.out.println(file.getAbsolutePath());
            tree.close();
            if (file.isDirectory())
                IOTools.shallowDeleteDirWithFiles((file));

        }
    }
}