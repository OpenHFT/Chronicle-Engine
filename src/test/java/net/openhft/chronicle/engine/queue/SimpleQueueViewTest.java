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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
    @NotNull
    private final WireType wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();
    @NotNull
    String methodName = "";
    private AssetTree assetTree;
    @Nullable
    private ServerEndpoint serverEndpoint;
    @Nullable
    private AssetTree serverAssetTree;

    public SimpleQueueViewTest(Boolean isRemote) {
        this.isRemote = isRemote;
        this.wireType = WireType.BINARY;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Boolean[][]{
                {true}, {false}
        });
    }

    public static void deleteFiles(@NotNull File element) {
        if (element.isDirectory()) {
            for (@NotNull File sub : element.listFiles()) {
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

            @NotNull String hostPortDescription = "SimpleQueueViewTest-methodName" + methodName + wireType;
            TCPRegistry.createServerSocketChannelFor(hostPortDescription);
            serverEndpoint = new ServerEndpoint(hostPortDescription, serverAssetTree);

            @NotNull final VanillaAssetTree client = new VanillaAssetTree();
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
        //YamlLogging.setAll(true);
        @NotNull String uri = "/queue/" + methodName;
        @NotNull String messageType = "topic";

        @NotNull TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri + DELETE_CHRONICLE_FILE, String.class, String.class);
        @NotNull BlockingQueue<String> values0 = new LinkedBlockingQueue<>();
        @Nullable Subscriber<String> subscriber = e -> {
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

        @Nullable QueueView<String, String> queueView = null;

        @NotNull String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        @NotNull String messageType = "topic";

        queueView = assetTree.acquireQueue(uri, String.class, String.class);
        Jvm.pause(500);
        final long index = queueView.publishAndIndex(messageType, "Message-1");
        @Nullable final Excerpt<String, String> actual = queueView.getExcerpt(index);
        assertEquals(index, actual.index());

        final long index2 = queueView.publishAndIndex(messageType, "Message-2");
        @Nullable final Excerpt<String, String> actual2 = queueView.getExcerpt(index2);
        assertEquals(index2, actual2.index());
    }

    @Test
    public void testStringPublish() throws InterruptedException {
        @Nullable Publisher<String> publisher = null;

        @NotNull String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, String.class);
        @NotNull BlockingQueue<String> values = new LinkedBlockingQueue<>();
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
        Publisher<String> publisher;

        //YamlLogging.setAll(true);
        @NotNull String uri = "/queue/" + methodName + System.nanoTime() + "/key" + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, String.class);
        @NotNull BlockingQueue<String> values = new ArrayBlockingQueue<>(2);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
        publisher.publish("Message-2");
        assertEquals("Message-2", values.poll(5, SECONDS));
        assertEquals("[]", values.toString());
    }

    @Test
    public void testStringPublishToAKeyTopicNotForMe() throws InterruptedException {
        @Nullable Publisher<String> publisher = null;

        @NotNull String uri = "/queue/" + methodName + "/key" + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, String.class);
        @NotNull BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
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

        @NotNull String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        @NotNull String messageType = "topic";
        publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        @NotNull BlockingQueue<String> values = new LinkedBlockingQueue<>();
        @NotNull TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
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
//        YamlLogging.showClientReads(true);
        @Nullable Publisher<String> publisher = null;
        @NotNull String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        @NotNull String messageType = "topic";

        // todo - fix
        if (!isRemote)
            assetTree.acquireQueue(uri, String.class, String.class);
        publisher = assetTree.acquirePublisher(uri + "/" + messageType, String.class);
        @NotNull BlockingQueue<String> values = new LinkedBlockingQueue<>();

        @NotNull TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
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
        @Nullable QueueView<String, String> publisher = null;

        @NotNull String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;

        publisher = assetTree.acquireQueue(uri, String.class, String
                .class);

        final long index = publisher.publishAndIndex(methodName, "Message-1");

        @NotNull QueueView<String, String> queue = assetTree.acquireQueue(uri, String.class, String.class);
        @Nullable final Excerpt<String, String> excerpt = queue.getExcerpt(index);
        assertEquals(methodName, excerpt.topic());
        assertEquals("Message-1", excerpt.message());
    }

    @Test
    public void testMarshablePublishToATopic() throws InterruptedException {
        @Nullable Publisher<MyMarshallable> publisher = null;

        @NotNull String uri = "/queue/" + methodName + DELETE_CHRONICLE_FILE;
        publisher = assetTree.acquirePublisher(uri, MyMarshallable.class);
        @NotNull BlockingQueue<MyMarshallable> values2 = new LinkedBlockingQueue<>();
        assetTree.registerSubscriber(uri, MyMarshallable.class, values2::add);
        publisher.publish(new MyMarshallable("Message-1"));
        publisher.publish(new MyMarshallable("Message-2"));

        assertEquals("Message-1", values2.poll(2, SECONDS).toString());
        assertEquals("Message-2", values2.poll(2, SECONDS).toString());
        Jvm.pause(100);
        assertEquals("[]", values2.toString());
    }
}

