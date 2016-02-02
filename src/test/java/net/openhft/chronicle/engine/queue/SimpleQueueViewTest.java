package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import static net.openhft.chronicle.engine.Chassis.*;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SimpleQueueViewTest extends ThreadMonitoringTest {

    private static final String NAME = "/test";

    private final boolean isRemote;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private final WireType wireType;
    private AssetTree assetTree;

    public SimpleQueueViewTest(Boolean isRemote) {
        this.isRemote = isRemote;
        wireType = WireType.TEXT;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        return Arrays.asList(new Boolean[][]{
                {false}
                , {true}
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
    public void before() {
        methodName(name.getMethodName());

        if (!isRemote)
            assetTree = (new VanillaAssetTree(1)).forTesting(
                    x -> t.set(x));
        else {
          /*  assetTree = (new VanillaAssetTree(1)).forRemoteAccess("192.168.1.76:8088",
                    wireType,
                    x -> t.set(x));*/
        }

    }

    @Test
    public void testMarshablePublishToATopic() throws InterruptedException {
        String uri = "/queue/" + name;
        Publisher<MyMarshallable> publisher = assetTree.acquirePublisher(uri, MyMarshallable.class);
        BlockingQueue<MyMarshallable> values = new ArrayBlockingQueue<>(1);
        Subscriber<MyMarshallable> subscriber = values::add;
        assetTree.registerSubscriber(uri, MyMarshallable.class, subscriber);
        publisher.publish(new MyMarshallable("Message-1"));
        assertEquals("Message-1", values.poll(2, SECONDS).toString());
    }

    @Test
    public void testStringPublishToATopic() throws InterruptedException {
        String uri = "/queue/" + name;
        acquireQueue(uri, String.class, String.class);
        Publisher<String> publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS).toString());
    }

    @Test

    public void testStringPublishToAKeyTopic() throws InterruptedException {
        String uri = "/queue/" + name + "/key";
        acquireQueue("/queue/" + name, String.class, String.class);
        Publisher<String> publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri, String.class, subscriber);
        publisher.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS).toString());
    }

    @Test

    public void testStringPublishToAKeyTopicNotForMe() throws InterruptedException {
        String uri = "/queue/" + name + "/key";
        acquireQueue("/queue/" + name, String.class, String.class);
        Publisher<String> publisher = assetTree.acquirePublisher(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = values::add;
        assetTree.registerSubscriber(uri + "KeyNotForMe", String.class, subscriber);
        publisher.publish("Message-1");
        assertEquals(null, values.poll(1, SECONDS));
    }

    @Test

    public void testStringTopicPublisherString() throws InterruptedException {
        String uri = "/queue/" + name;
        String messageType = "topic";
        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);
        publisher.publish(messageType, "Message-1");
        assertEquals("topic Message-1", values.poll(2, SECONDS).toString());
    }

    @Test

    public void testStringTopicPublisherWithSubscribe() throws InterruptedException {
        String uri = "/queue/" + name;
        String messageType = "topic";
        TopicPublisher<String, String> publisher = assetTree.acquireTopicPublisher(uri, String.class, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = e -> {
            if (e != null)
                values.add(e);
        };

        assetTree.registerSubscriber(uri + "/" + messageType, String.class, subscriber);
        publisher.publish(messageType, "Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS).toString());
    }

    @Test

    public void testSrtringPublishWithTopicSubscribe() throws InterruptedException {
        String uri = "/queue/" + name;
        String messageType = "topic";
        assetTree.acquireQueue(uri, String.class, String.class);
        Publisher<String> publisher =  assetTree.acquirePublisher(uri + "/" + messageType, String
                .class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);

        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);
        publisher.publish("Message-1");
        assertEquals("topic Message-1", values.poll(2, SECONDS).toString());
    }


    @Test
    public void testStringPublishWithIndex() throws InterruptedException, IOException {
        String uri = "/queue/" + name;

        final Publisher<String> publisher = assetTree.acquirePublisher(uri,
                String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);

        long index = publisher.publish("Message-1");
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add(topic + " " + message);
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);

        QueueView<String, String> queue = assetTree.acquireQueue(uri, String.class, String.class);
        queue.replay(index, (topic, message) -> values.add(topic + " " + message), null);
        assertEquals(name + " " + "Message-1", values.poll(2, SECONDS).toString());
    }

    class MyMarshallable implements Marshallable {

        String s;

        public MyMarshallable(String s) {
            this.s = s;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
            s = wire.read().text();
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write().text(s);
        }

        @Override
        public String toString() {
            return s;
        }
    }

}

