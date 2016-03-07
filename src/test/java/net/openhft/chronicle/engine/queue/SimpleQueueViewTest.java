package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.QueueView.Excerpt;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SimpleQueueViewTest extends ThreadMonitoringTest {

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(MyMarshallable.class, "MyMarshallable");
    }

    private static final String NAME = "/test";

    private final boolean isRemote;
    @NotNull
    @Rule
    public TestName name = new TestName();
    String methodName = "";
    private final WireType wireType;
    private AssetTree assetTree;
    private ServerEndpoint serverEndpoint;

    public SimpleQueueViewTest(Boolean isRemote) {
        this.isRemote = isRemote;
        wireType = WireType.BINARY;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        return Arrays.asList(new Boolean[][]{
                {false}, {true}
        });
    }

    @AfterClass
    public static void tearDownClass() {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        final Throwable th = t.getAndSet(null);
        if (th != null) Jvm.rethrow(th);
    }


    private static AtomicReference<Throwable> t = new AtomicReference();


    @Before
    public void before() throws IOException {
        methodName(name.getMethodName());
        methodName = name.getMethodName().substring(0, name.getMethodName().indexOf('['));

        if (isRemote) {
            final VanillaAssetTree server = new VanillaAssetTree();
            final AssetTree serverAssetTree = server.forTesting(x -> t.set(x));

            String hostPortDescription = "SimpleQueueViewTest-methodName" + methodName + wireType;
            TCPRegistry.createServerSocketChannelFor(hostPortDescription);
            serverEndpoint = new ServerEndpoint(hostPortDescription, serverAssetTree);

            final VanillaAssetTree client = new VanillaAssetTree();
            assetTree = client.forRemoteAccess(hostPortDescription,
                    WireType.BINARY, x -> t.set(x));


        } else {
            assetTree = (new VanillaAssetTree(1)).forTesting(x -> t.set(x));
            serverEndpoint = null;
        }

        YamlLogging.setAll(true);
    }


    @After
    public void after() throws Throwable {
        final Throwable tr = t.getAndSet(null);

        if (tr != null)
            throw tr;

        if (serverEndpoint != null)
            serverEndpoint.close();

        assetTree.close();
        methodName = "";
        TCPRegistry.reset();
    }


    @Test
    public void testStringTopicPublisherWithSubscribe() throws InterruptedException {

        String uri = "/queue/" + methodName;
        String messageType = "topic";

        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> values0 = new ArrayBlockingQueue<>(10);
        Subscriber<String> subscriber = e -> {
            if (e != null) {
                values0.add(e);
            }
        };

        assetTree.registerSubscriber(uri, String.class, subscriber);
        Thread.sleep(500);
        publisher.publish(messageType, "Message-1");
        assertEquals("Message-1", values0.poll(3, SECONDS));
    }


    @Test
    public void testPublishAndNext() throws InterruptedException {

        String uri = "/queue/" + methodName;
        String messageType = "topic";

        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);

        final QueueView<String, String> queueView = assetTree.acquireQueue(uri, String.class,
                String.class);
        Thread.sleep(500);
        publisher.publish(messageType, "Message-1");
        final Excerpt<String, String> next = queueView.next();
        assertEquals("Message-1", next.message());
    }


    @Test
    public void testPublishAtIndexCheckIndex() throws InterruptedException {

        String uri = "/queue/" + methodName;
        String messageType = "topic";

        final QueueView<String, String> queueView = assetTree.acquireQueue(uri, String.class,
                String.class);
        Thread.sleep(500);
        final long index = queueView.publishAndIndex(messageType, "Message-1");
        final Excerpt<String, String> actual = queueView.next();
        assertEquals(index, actual.index());
    }


    @Test
    public void testStringPublishToATopic() throws InterruptedException {
        String uri = "/queue/testStringPublishToATopic";
        Publisher<String> publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        Thread.sleep(500);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
    }


    @Test
    public void testStringPublishToAKeyTopic() throws InterruptedException {
        YamlLogging.setAll(true);
        String uri = "/queue/" + methodName + "/key";
        Publisher<String> publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(10);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        Thread.sleep(500);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
    }

    @Test
    public void testStringPublishToAKeyTopicNotForMe() throws InterruptedException {
        String uri = "/queue/" + methodName + "/key";
        Publisher<String> publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri + "KeyNotForMe", String.class, subscriber);
        Thread.sleep(500);
        publisher.publish("Message-1");
        assertEquals(null, values.poll(1, SECONDS));
    }

    @Test

    public void testStringTopicPublisherString() throws InterruptedException {
        String uri = "/queue/" + methodName;
        String messageType = "topic";
        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(10);
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);
        Thread.sleep(500);
        publisher.publish(messageType, "Message-1");
        assertEquals("topic Message-1", values.poll(2, SECONDS));
    }


    @Test
    public void testSrtringPublishWithTopicSubscribe() throws InterruptedException {
        String uri = "/queue/" + methodName;
        String messageType = "topic";

        // todo - fix
        if (!isRemote)
            assetTree.acquireQueue(uri, String.class, String.class);
        Publisher<String> publisher = assetTree.acquirePublisher(uri + "/" + messageType, String
                .class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(10);

        TopicSubscriber<String, String> subscriber = (topic, message) -> {
            values.add(topic + " " + message);
        };
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);

        publisher.publish("Message-1");
        assertEquals("topic Message-1", values.poll(20, SECONDS));
    }


    @Test
    public void testStringPublishWithIndex() throws InterruptedException, IOException {

        // todo - replay is not currently supported remotely

        String uri = "/queue/" + methodName;

        final QueueView<String, String> publisher = assetTree.acquireQueue(uri, String.class, String
                .class);

        final long index = publisher.publishAndIndex(methodName, "Message-1");

        QueueView<String, String> queue = assetTree.acquireQueue(uri, String.class, String.class);
        final Excerpt<String, String> excerpt = queue.get(index);
        assertEquals(methodName, excerpt.topic());
        assertEquals("Message-1", excerpt.message());
    }


    @Test
    public void testMarshablePublishToATopic() throws InterruptedException {
        String uri = "/queue/testMarshablePublishToATopic";
        Publisher<MyMarshallable> publisher = assetTree.acquirePublisher(uri, MyMarshallable.class);
        BlockingQueue<MyMarshallable> values2 = new ArrayBlockingQueue<>(10);
        assetTree.registerSubscriber(uri, MyMarshallable.class, new Subscriber<MyMarshallable>() {
            @Override
            public void onMessage(MyMarshallable e) throws InvalidSubscriberException {

                values2.add(e);
            }
        });
        publisher.publish(new MyMarshallable("Message-1"));

        assertEquals("Message-1", values2.poll(5, SECONDS).toString());
    }


}

