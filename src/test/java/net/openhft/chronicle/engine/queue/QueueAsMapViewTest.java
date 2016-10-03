package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.engine.Utils.methodName;

/**
 * @author Rob Austin.
 */

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
    }

    @Test
    public void testQueueViewAsMapView() throws InterruptedException {
        YamlLogging.setAll(true);
        String uri = "/queue/" + methodName;
        deleteFiles(new File(uri));

        String messageType1 = "topic1";
        String messageType2 = "topic2";

        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri + DELETE_CHRONICLE_FILE, String.class, String.class);
        publisher.publish(messageType1, "Message-1");
        publisher.publish(messageType2, "Message-2");

        Jvm.pause(500);

        MapView<String, String> map = assetTree.acquireMap(uri, String.class, String.class);
        int i = 0;
        while (map.size() == 0) {
            if (i++ == 1000)
                break;
            Thread.sleep(100);
        }
        Assert.assertEquals(2, map.size());

    }


}

