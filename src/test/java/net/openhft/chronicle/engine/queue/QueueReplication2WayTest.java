package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.ChronicleMapKeyValueStoreTest;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.queue.SimpleQueueViewTest.deleteFile;
import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */

@RunWith(value = Parameterized.class)
public class QueueReplication2WayTest {

    private final WireType wireType;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        final int initialCapacity = 1;
        final List<Object[]> list = new ArrayList<>(initialCapacity);

        for (int i = 0; i < initialCapacity; i++) {
            list.add(new Object[]{WireType.BINARY});
        }

        return list;
    }

    public QueueReplication2WayTest(WireType wireType) {
        this.wireType = wireType;
    }

    String methodName;

    @Rule
    public TestName name = new TestName();


    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String NAME = "/ChMaps/test";
    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;

    private static AssetTree tree1;
    private static AssetTree tree2;
    private static AtomicReference<Throwable> t = new AtomicReference();

    @Before
    public void before() throws IOException, InterruptedException {
        YamlLogging.setAll(false);

        methodName(name.getMethodName());
        methodName = name.getMethodName().substring(0, name.getMethodName().indexOf('['));

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));


        TCPRegistry.createServerSocketChannelFor(
                "clusterThree.host.port1",
                "clusterThree.host.port2",
                "clusterThree.host.port3",
                "host.port1",
                "host.port2",
                "host.port3");

        tree1 = create(1, wireType, "clusterTwo");
        tree2 = create(2, wireType, "clusterTwo");

        serverEndpoint1 = new ServerEndpoint("host.port1", tree1);
        serverEndpoint2 = new ServerEndpoint("host.port2", tree2);
    }

    @After
    public void after() throws IOException, InterruptedException {


        if (serverEndpoint1 != null)
            serverEndpoint1.close();
        if (serverEndpoint2 != null)
            serverEndpoint2.close();

        for (AssetTree tree : new AssetTree[]{tree1, tree2}) {
            if (tree == null)
                continue;

            try {
                final ChronicleQueueView queueView = (ChronicleQueueView) tree.acquireAsset("/queue/" + methodName).acquireView(QueueView.class);

                //    System.out.println(queueView.dump());

                final File path = queueView.chronicleQueue().path();
                System.out.println("path=" + path);
                deleteFile(path);
            } catch (Exception ignore) {

            }
            tree.close();
        }


        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();

    }

    @NotNull
    private static AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x))
                .withConfig(resourcesDir() + "/cmkvst", OS.TARGET + "/" + hostId);
        final Asset queue = tree.root().acquireAsset("queue");
        queue.addLeafRule(QueueView.class, VanillaAsset.LAST + "chronicle queue", (context, asset) ->
                new ChronicleQueueView(context.wireType(writeType).cluster(clusterTwo), asset));


//        VanillaAssetTreeEgMain.registerTextViewofTree("host " + hostId, tree);

        return tree;
    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }


    @Test
    public void testTwoWay() throws InterruptedException {
        YamlLogging.setAll(true);

        final String uri = "/queue/" + methodName;
        final Publisher<String> publisher = tree1.acquirePublisher(uri, String.class);

        final BlockingQueue<String> tree2Values = new ArrayBlockingQueue<>(10);

        tree2.registerSubscriber(uri, String.class, message -> {
            tree2Values.add(message);
        });


        publisher.publish("Message-1");
        assertEquals("Message-1", tree2Values.poll(2, SECONDS));
    }

    @Test
    public void testStringTopicPublisherString() throws InterruptedException {
        YamlLogging.setAll(true);

        String uri = "/queue/" + methodName;
        String messageType = "topic";
        TopicPublisher<String, String> publisher = tree1.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> tree2Values = new ArrayBlockingQueue<>(10);


        tree2.registerTopicSubscriber(uri, String.class, String.class, (topic, message) ->
                tree2Values.add(topic + " " + message));

        Thread.sleep(500);
        publisher.publish(messageType, "Message-1");
        assertEquals("topic Message-1", tree2Values.poll(2, SECONDS));

    }

    @Test(expected = IllegalStateException.class)
    public void testPublishingToSycnThrowsError() throws InterruptedException {
        String uri = "/queue/" + methodName;
        String messageType = "topic";
        TopicPublisher<String, String> publisher = tree2.acquireTopicPublisher(uri, String.class,
                String.class);

        publisher.publish(messageType, "Message-1");

    }

}

