/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */

@RunWith(value = Parameterized.class)
public class SimpleQueueViewTest extends ThreadMonitoringTest {

    private static final String DELETE_CHRONICLE_FILE = "?dontPersist=true";

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
    private AssetTree serverAssetTree;

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
        methodName = name.getMethodName()
                .substring(0, name.getMethodName().indexOf('['))
                + "-" + System.nanoTime();

        if (isRemote) {
            serverAssetTree = new VanillaAssetTree().forTesting();

            String hostPortDescription = "SimpleQueueViewTest-methodName" + methodName + wireType;
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
    public void testStringTopicPublisherWithSubscribe() throws InterruptedException {
        YamlLogging.setAll(true);
        String uri = "/queue/" + methodName;
        String messageType = "topic";

        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri + DELETE_CHRONICLE_FILE, String.class, String.class);
        BlockingQueue<String> values0 = new LinkedBlockingQueue<>();
        Subscriber<String> subscriber = e -> {
            if (e != null) {
                values0.add(e);
            }
        };
        publisher.publish(messageType, "Message-1");
        publisher.publish(messageType, "Message-2");

        Jvm.pause(500);

        assetTree.registerSubscriber(uri + "/" + messageType + DELETE_CHRONICLE_FILE, String.class, subscriber);

        assertEquals("Message-1", values0.poll(3, SECONDS));
        assertEquals("Message-2", values0.poll(3, SECONDS));
        Jvm.pause(100);
        assertEquals("[]", values0.toString());
    }

    @Test
    public void testPublishAtIndexCheckIndex() throws InterruptedException {

        QueueView<String, String> queueView = null;

        String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        String messageType = "topic";

        queueView = assetTree.acquireQueue(uri, String.class, String.class);
        Jvm.pause(500);
        final long index = queueView.publishAndIndex(messageType, "Message-1");
        final Excerpt<String, String> actual = queueView.get(index);
        assertEquals(index, actual.index());

        final long index2 = queueView.publishAndIndex(messageType, "Message-2");
        final Excerpt<String, String> actual2 = queueView.get(index2);
        assertEquals(index2, actual2.index());
    }

    @Test
    public void testStringPublish() throws InterruptedException {
        Publisher<String> publisher = null;

        String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new LinkedBlockingQueue<>();
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        Jvm.pause(500);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
        publisher.publish("Message-2");
        assertEquals("Message-2", values.poll(2, SECONDS));
        Jvm.pause(100);
        assertEquals("[]", values.toString());
    }

    @Test
    public void testStringPublishToAKeyTopic() throws InterruptedException {
        Publisher<String> publisher = null;

        YamlLogging.setAll(true);
        String uri = "/queue/" + methodName + "/key" + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        Jvm.pause(500);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
        publisher.publish("Message-2");
        assertEquals("Message-2", values.poll(2, SECONDS));
        Jvm.pause(100);
        assertEquals("[]", values.toString());
    }

    @Test
    public void testStringPublishToAKeyTopicNotForMe() throws InterruptedException {
        Publisher<String> publisher = null;

        String uri = "/queue/" + methodName + "/key" + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber("/queue/" + methodName + "/keyNotForMe", String.class, subscriber);
        Jvm.pause(200);
        publisher.publish("Message-1");
        assertEquals(null, values.poll(200, MILLISECONDS));
        publisher.publish("Message-2");
        assertEquals(null, values.poll(200, MILLISECONDS));
    }

    @Test
    @Ignore("TODO FIX Too many results")
    public void testStringTopicPublisherString() throws InterruptedException {
        TopicPublisher<String, String> publisher;

        String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        String messageType = "topic";
        publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> values = new LinkedBlockingQueue<>();
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);
        Jvm.pause(200);
        publisher.publish(messageType, "Message-1");
        assertEquals("topic Message-1", values.poll(2, SECONDS));
        publisher.publish(messageType, "Message-2");
        assertEquals("topic Message-2", values.poll(2, SECONDS));
        Jvm.pause(200);
        assertEquals("[]", values.toString());
    }

    @Test
    @Ignore("TODO FIX Too many results")
    public void testStringPublishWithTopicSubscribe() throws InterruptedException {
        YamlLogging.showClientReads(true);
        Publisher<String> publisher = null;
        String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        String messageType = "topic";

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
    }

    @Test
    public void testStringPublishWithIndex() throws InterruptedException, IOException {
        QueueView<String, String> publisher = null;

        String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;

        publisher = assetTree.acquireQueue(uri, String.class, String
                .class);

        final long index = publisher.publishAndIndex(methodName, "Message-1");

        QueueView<String, String> queue = assetTree.acquireQueue(uri, String.class, String.class);
        final Excerpt<String, String> excerpt = queue.get(index);
        assertEquals(methodName, excerpt.topic());
        assertEquals("Message-1", excerpt.message());
    }

    @Test
    public void testMarshablePublishToATopic() throws InterruptedException {
        Publisher<MyMarshallable> publisher = null;

        String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, MyMarshallable.class);
        BlockingQueue<MyMarshallable> values2 = new LinkedBlockingQueue<>();
        assetTree.registerSubscriber(uri, MyMarshallable.class, values2::add);
        publisher.publish(new MyMarshallable("Message-1"));
        publisher.publish(new MyMarshallable("Message-2"));

        assertEquals("Message-1", values2.poll(2, SECONDS).toString());
        assertEquals("Message-2", values2.poll(2, SECONDS).toString());
        Jvm.pause(100);
        assertEquals("[]", values2.toString());
    }
}

