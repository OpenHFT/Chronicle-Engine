package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */

public class LocalQueueRefTest extends ThreadMonitoringTest {


    @NotNull
    @Rule
    public TestName name = new TestName();
    String methodName = "";

    private AssetTree assetTree;

    private static AtomicReference<Throwable> t = new AtomicReference();


    @Before
    public void before() throws IOException {
        methodName(name.getMethodName());
        methodName = name.getMethodName();
        assetTree = (new VanillaAssetTree(1)).forTesting(
                x -> t.set(x));
        YamlLogging.setAll(true);
    }


    @After
    public void after() {
        methodName = "";
    }

    @Test
    public void test() throws InterruptedException {
        String uri = "/queue/" + methodName;

        final Reference<String> ref = assetTree.acquireReference(uri, String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        Subscriber<String> subscriber = e -> {
            if (e != null)
                values.add(e);
        };

        assetTree.registerSubscriber(uri, String.class, subscriber);
        ref.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
    }


    @Test
    public void test2() throws InterruptedException {
        String uri = "/queue/" + methodName;
        assetTree.acquireQueue(uri, String.class, String.class);
        final Reference<String> ref = assetTree.acquireReference(uri + "/key", String.class);
        BlockingQueue<String> values = new ArrayBlockingQueue<>(1);
        TopicSubscriber<String, String> subscriber = (topic, message) -> {
            if (message != null)
                values.add(message);
        };
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);

        ref.publish("Message-1");
        assertEquals("Message-1", values.poll(2, SECONDS));
    }

}

