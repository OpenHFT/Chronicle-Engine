package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.engine.Utils.methodName;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class QueueAsMapViewTest extends ThreadMonitoringTest {


    private static final String DELETE_CHRONICLE_FILE = "?dontPersist=true";

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(MyMarshallable.class, "MyMarshallable");
    }


    @NotNull
    @Rule
    public TestName name = new TestName();
    String methodName = "";
    private AssetTree assetTree;
    private ServerEndpoint serverEndpoint;
    private AssetTree serverAssetTree;

    String uri = "/queue/" + System.nanoTime() + "/" + methodName;


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Boolean[][]{
                {true}, {false}
        });
    }

    public QueueAsMapViewTest(Boolean isRemote) throws Exception {

        if (isRemote) {
            serverAssetTree = new VanillaAssetTree().forTesting();

            String hostPortDescription = "SimpleQueueViewTest-methodName" + methodName;
            TCPRegistry.createServerSocketChannelFor(hostPortDescription);
            serverEndpoint = new ServerEndpoint(hostPortDescription, serverAssetTree);

            final VanillaAssetTree client = new VanillaAssetTree();
            assetTree = client.forRemoteAccess(hostPortDescription, WireType.BINARY);

        } else {
            assetTree = (new VanillaAssetTree(1)).forTesting();
            serverEndpoint = null;
            serverAssetTree = null;
        }

    }


    public static void deleteFiles(File element) {
        if (element.isDirectory()) {
            for (File sub : element.listFiles()) {
                deleteFiles(sub);
            }
        }
        element.delete();
    }

    @Before
    public void before() throws IOException {

        methodName(name.getMethodName());
        deleteFiles(new File(uri));
        assetTree = (new VanillaAssetTree(1)).forTesting();
        serverEndpoint = null;
        serverAssetTree = null;

    }

    @After
    public void preAfter() {

        threadDump.ignore("ChronicleMapKeyValueStore Closer");
        Closeable.closeQuietly(serverAssetTree);
        Closeable.closeQuietly(serverEndpoint);
        Closeable.closeQuietly(assetTree);

        methodName = "";

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        deleteFiles(new File(uri));
    }

    @Test
    public void testQueueViewAsMapView() throws InterruptedException {
        YamlLogging.setAll(true);
        String messageType1 = "topic1";
        String messageType2 = "topic2";

        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri + DELETE_CHRONICLE_FILE, String.class, String.class);
        publisher.publish(messageType1, "Message-1");
        publisher.publish(messageType2, "Message-2");

        Jvm.pause(500);

        MapView<String, String> map = assetTree.acquireMap(uri, String.class, String.class);
        Assert.assertTrue(map instanceof ChronicleQueueView);

        int i = 0;
        while (map.size() == 0) {
            if (i++ == 500)
                break;
            Thread.sleep(100);
        }
        Assert.assertEquals(2, map.size());

    }

    @Test
    public void testSimpleMap() throws InterruptedException {
        YamlLogging.setAll(true);

        MapView<String, String> map = assetTree.acquireMap(uri, String.class, String.class);
        map.put("hello", "world");
        map.put("hello2", "world");
        Assert.assertEquals(2, map.size());

    }

    @Test
    public void testPopulateMapTailQueue() throws InterruptedException {
        YamlLogging.setAll(true);

        final ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(100);

        assetTree.registerTopicSubscriber(
                uri,
                String.class,
                String.class, (topic, message) -> q.add(topic + ":" + message));

        MapView<String, String> map = assetTree.acquireMap(uri, String.class, String.class);
        Assert.assertTrue(map instanceof ChronicleQueueView);
        map.put("hello", "world");
        map.put("hello2", "world2");

        Assert.assertEquals("hello:world", q.poll(1, TimeUnit.SECONDS));
        Assert.assertEquals("hello2:world2", q.poll(1, TimeUnit.SECONDS));

        Assert.assertTrue(q.isEmpty());


    }


}
