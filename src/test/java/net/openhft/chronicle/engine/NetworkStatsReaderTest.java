package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.queue.SimpleQueueViewTest;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.server.internal.NetworkStatsSummary;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.NetworkStats;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.WireNetworkStats;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class NetworkStatsReaderTest {

    @NotNull
    @Rule
    public TestName name = new TestName();

    private AssetTree assetTree;
    public static final String URI = "queue/networkStats";


    @Before
    public void before() throws IOException {
        SimpleQueueViewTest.deleteFiles(new File(URI));
        assetTree = (new VanillaAssetTree(1)).forServer();

        String hostPortDescription = "NetworkStatsReaderTest-" + name;
        TCPRegistry.createServerSocketChannelFor(hostPortDescription);
        new ServerEndpoint(hostPortDescription, assetTree);


    }

    @Test
    public void test() throws Exception {

        YamlLogging.setAll(true);

        EventLoop eg = assetTree.root().findOrCreateView(EventLoop.class);
        eg.start();
        MapView<String, NetworkStatsSummary.Stats> mapView = assetTree.acquireMap("myStats", String.class, NetworkStatsSummary.Stats.class);


        ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
        mapView.registerSubscriber((e) -> queue.add(e.getValue().toString()));

        eg.addHandler(new NetworkStatsSummary((ChronicleQueueView) assetTree.acquireAsset(URI).acquireView(QueueView.class), mapView));

        {
            TopicPublisher<String, NetworkStats> publisher = assetTree.acquireTopicPublisher(URI,
                    String.class, NetworkStats.class);
            WireNetworkStats networkStats = new WireNetworkStats(0);
            networkStats.clientId(UUID.randomUUID());
            networkStats.isConnected(true);
            publisher.publish("NetworkStats", networkStats.writeBps(1).userId("1"));
            publisher.publish("NetworkStats", networkStats.writeBps(2).timestamp(1000));
            publisher.publish("NetworkStats", networkStats.writeBps(3).timestamp(2000));
            publisher.publish("NetworkStats", networkStats.writeBps(4).timestamp(3000));
        }

        String result = "";
        for (; ; ) {
            String pollValue = queue.poll(1, TimeUnit.SECONDS);
            if (pollValue == null) {
                Assert.assertTrue(result.contains("  writeEma: 3.98"));
                break;
            }
            result = pollValue;
            System.out.println(result);
        }

    }

}
