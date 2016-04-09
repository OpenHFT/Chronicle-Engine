/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.pubsub.QueueReference;
import net.openhft.chronicle.engine.pubsub.QueueTopicPublisher;
import net.openhft.chronicle.engine.pubsub.RemotePublisher;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SimpleQueueViewTest extends ThreadMonitoringTest {

    private static final String NAME = "/test";
    private static AtomicReference<Throwable> t = new AtomicReference();

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(MyMarshallable.class, "MyMarshallable");
    }

    private final boolean isRemote;
    private final WireType wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();
    String methodName = "";
    private AssetTree assetTree;
    private ServerEndpoint serverEndpoint;

    public SimpleQueueViewTest(Boolean isRemote) {
        this.isRemote = isRemote;
        this.wireType = WireType.BINARY;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        return Arrays.asList(new Boolean[][]{
                {true}, {true}
        });
    }


    @AfterClass
    public static void tearDownClass() {


        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
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
        methodName = name.getMethodName().substring(0, name.getMethodName().indexOf('['));

        if (isRemote) {
            final VanillaAssetTree server = new VanillaAssetTree();
            final AssetTree serverAssetTree = server.forTesting(x -> {
                t.set(x);
                x.printStackTrace();
            });

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

        YamlLogging.setAll(false);
    }

    @After
    public void after() throws Throwable {


        if (serverEndpoint != null)
            serverEndpoint.close();

        if (assetTree != null)
            assetTree.close();
        methodName = "";
        TCPRegistry.reset();

        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }


    // todo fix for remote
    @Test
    @Ignore("TODO FIX Too many results")
    public void testStringTopicPublisherWithSubscribe() throws InterruptedException {
        YamlLogging.setAll(true);
        String uri = "/queue/" + methodName + System.nanoTime();
        String messageType = "topic";

        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> values0 = new LinkedBlockingQueue<>();
        Subscriber<String> subscriber = e -> {
            if (e != null) {
                values0.add(e);
            }
        };
        publisher.publish(messageType, "Message-1");
        publisher.publish(messageType, "Message-2");

        Thread.sleep(1000);

        assetTree.registerSubscriber(uri + "/" + messageType, String.class, subscriber);

        assertEquals("Message-1", values0.poll(3, SECONDS));
        assertEquals("Message-2", values0.poll(3, SECONDS));
        Jvm.pause(100);
        assertEquals("[]", values0.toString());
        deleteFiles(publisher);
    }


    @Test
    public void testPublishAtIndexCheckIndex() throws InterruptedException {

        QueueView<String, String> queueView = null;
        try {
            String uri = "/queue/" + methodName + System.nanoTime();
            String messageType = "topic";

            queueView = assetTree.acquireQueue(uri, String.class, String.class);
            Thread.sleep(500);
            final long index = queueView.publishAndIndex(messageType, "Message-1");
            final Excerpt<String, String> actual = queueView.get(index);
            assertEquals(index, actual.index());

            final long index2 = queueView.publishAndIndex(messageType, "Message-2");
            final Excerpt<String, String> actual2 = queueView.get(index2);
            assertEquals(index2, actual2.index());
        } finally {
            deleteFiles(queueView);
        }
    }

    @Test
    public void testStringPublish() throws InterruptedException {
        Publisher<String> publisher = null;
        try {
            String uri = "/queue/testStringPublishToATopic" + System.nanoTime();
            publisher = assetTree.acquirePublisher(uri, String.class);
            BlockingQueue<String> values = new LinkedBlockingQueue<>();
            Subscriber<String> subscriber = values::add;
            assetTree.registerSubscriber(uri, String.class, subscriber);
            Thread.sleep(500);
            publisher.publish("Message-1");
            assertEquals("Message-1", values.poll(2, SECONDS));
            publisher.publish("Message-2");
            assertEquals("Message-2", values.poll(2, SECONDS));
            Jvm.pause(100);
            assertEquals("[]", values.toString());
        } finally {
            deleteFiles(publisher);
        }

    }


    @Test
    public void testStringPublishToAKeyTopic() throws InterruptedException {
        Publisher<String> publisher = null;
        try {
            YamlLogging.setAll(false);
            String uri = "/queue/" + methodName + "/key" + System.nanoTime();
            publisher = assetTree.acquirePublisher(uri, String.class);
            BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
            Subscriber<String> subscriber = values::add;
            assetTree.registerSubscriber(uri, String.class, subscriber);
            Thread.sleep(500);
            publisher.publish("Message-1");
            assertEquals("Message-1", values.poll(2, SECONDS));
            publisher.publish("Message-2");
            assertEquals("Message-2", values.poll(2, SECONDS));
            Jvm.pause(100);
            assertEquals("[]", values.toString());
        } finally {
            deleteFiles(publisher);
        }
    }

    @Test
    public void testStringPublishToAKeyTopicNotForMe() throws InterruptedException {

        Publisher<String> publisher = null;
        try {
            String uri = "/queue/" + methodName + "/key";
            publisher = assetTree.acquirePublisher(uri, String.class);
            BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
            Subscriber<String> subscriber = values::add;
            assetTree.registerSubscriber(uri + "KeyNotForMe", String.class, subscriber);
            Thread.sleep(200);
            publisher.publish("Message-1");
            assertEquals(null, values.poll(200, MILLISECONDS));
            publisher.publish("Message-2");
            assertEquals(null, values.poll(200, MILLISECONDS));
        } finally {
            deleteFiles(publisher);
        }
    }

    @Test
    @Ignore("TODO FIX Too many results")
    public void testStringTopicPublisherString() throws InterruptedException {
        TopicPublisher<String, String> publisher = null;
        try {
            String uri = "/queue/" + methodName + System.nanoTime();
            String messageType = "topic";
            publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
            BlockingQueue<String> values = new LinkedBlockingQueue<>();
            TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
            assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);
            Thread.sleep(200);
            publisher.publish(messageType, "Message-1");
            assertEquals("topic Message-1", values.poll(2, SECONDS));
            publisher.publish(messageType, "Message-2");
            assertEquals("topic Message-2", values.poll(2, SECONDS));
            Jvm.pause(200);
            assertEquals("[]", values.toString());
        } finally {
            deleteFiles(publisher);
        }
    }


    @Test
    @Ignore("TODO FIX Too many results")
    public void testStringPublishWithTopicSubscribe() throws InterruptedException {
        Publisher<String> publisher = null;
        String uri = "/queue/" + methodName + "-" + System.nanoTime();
        String messageType = "topic";
        try {
            // todo - fix
            if (!isRemote)
                assetTree.acquireQueue(uri, String.class, String.class);
            publisher = assetTree.acquirePublisher(uri + "/" + messageType, String.class);
            BlockingQueue<String> values = new LinkedBlockingQueue<>();

            TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
            assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);

            publisher.publish("Message-1");
            publisher.publish("Message-2");
            assertEquals("topic Message-1", values.poll(2, SECONDS));
            assertEquals("topic Message-2", values.poll(2, SECONDS));
            Jvm.pause(100);
            assertEquals("", values.toString());
        } finally {
            deleteFiles(publisher);
        }
    }

    @Test
    public void testStringPublishWithIndex() throws InterruptedException, IOException {
        QueueView<String, String> publisher = null;
        // todo - replay is not currently supported remotely
        try {
            String uri = "/queue/" + methodName + System.nanoTime();
            ;

            publisher = assetTree.acquireQueue(uri, String.class, String
                    .class);

            final long index = publisher.publishAndIndex(methodName, "Message-1");

            QueueView<String, String> queue = assetTree.acquireQueue(uri, String.class, String.class);
            final Excerpt<String, String> excerpt = queue.get(index);
            assertEquals(methodName, excerpt.topic());
            assertEquals("Message-1", excerpt.message());
        } finally {
            deleteFiles(publisher);
        }
    }

    @Test
    public void testMarshablePublishToATopic() throws InterruptedException {
        Publisher<MyMarshallable> publisher = null;
        try {
            String uri = "/queue/testMarshablePublishToATopic" + System.nanoTime();
            publisher = assetTree.acquirePublisher(uri, MyMarshallable.class);
            BlockingQueue<MyMarshallable> values2 = new LinkedBlockingQueue<>();
            assetTree.registerSubscriber(uri, MyMarshallable.class, values2::add);
            publisher.publish(new MyMarshallable("Message-1"));
            publisher.publish(new MyMarshallable("Message-2"));

            assertEquals("Message-1", values2.poll(2, SECONDS).toString());
            assertEquals("Message-2", values2.poll(2, SECONDS).toString());
            Jvm.pause(100);
            assertEquals("[]", values2.toString());
        } finally {
            deleteFiles(publisher);
        }
    }

    private void deleteFiles(Publisher p) {
        if (p instanceof RemotePublisher)
            return;
        try {
            Field chronicleQueue = QueueReference.class.getDeclaredField("chronicleQueue");
            chronicleQueue.setAccessible(true);
            ChronicleQueueView qv = (ChronicleQueueView) chronicleQueue.get(p);
            deleteFiles(qv);
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }

    private void deleteFiles(QueueView qv) {
        if (!(qv instanceof ChronicleQueueView))
            return;
        File path = ((ChronicleQueueView) qv).chronicleQueue().file();
        deleteFiles(path);
    }

    private void deleteFiles(ChronicleQueueView qv) {
        File path = qv.chronicleQueue().file();
        deleteFiles(path);
    }

    private void deleteFiles(TopicPublisher p) {

        if (p instanceof QueueTopicPublisher)
            deleteFiles((ChronicleQueueView) ((QueueTopicPublisher) p).underlying());

    }

}

