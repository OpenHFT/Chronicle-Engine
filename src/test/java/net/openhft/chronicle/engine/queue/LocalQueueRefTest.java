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
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */

public class LocalQueueRefTest extends ThreadMonitoringTest {

    private static AtomicReference<Throwable> t = new AtomicReference<>();
    @NotNull
    @Rule
    public TestName name = new TestName();
    String methodName = "";
    private AssetTree assetTree;

    @Before
    public void before() throws IOException {
        methodName(name.getMethodName());
        methodName = name.getMethodName();
        assetTree = (new VanillaAssetTree(1)).forTesting(
                x -> {
                    t.set(x);
                    x.printStackTrace();
                });
        YamlLogging.setAll(false);
    }

    @After
    public void after() {
        methodName = "";
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Test
    @Ignore("TODO FIX too many results")
    public void test() throws InterruptedException {
        String uri = "/queue/" + methodName;

        final Reference<String> ref = assetTree.acquireReference(uri, String.class);
        BlockingQueue<String> values = new LinkedBlockingQueue<>();
        Subscriber<String> subscriber = e -> {
            if (e != null)
                values.add(e);
        };

        assetTree.registerSubscriber(uri, String.class, subscriber);
        ref.publish("Message-1");
        ref.publish("Message-2");
        assertEquals("Message-1", values.poll(2, SECONDS));
        assertEquals("Message-2", values.poll(2, SECONDS));
        Jvm.pause(100);
        assertEquals("[]", values.toString());
    }

    @Test
    @Ignore("TODO FIX too many results")
    public void test2() throws InterruptedException {
        String uri = "/queue/" + methodName;
        assetTree.acquireQueue(uri, String.class, String.class);
        final Reference<String> ref = assetTree.acquireReference(uri + "/key", String.class);
        BlockingQueue<String> values = new LinkedBlockingQueue<>();
        TopicSubscriber<String, String> subscriber = (topic, message) -> {
            if (message != null)
                values.add(message);
        };
        assetTree.registerTopicSubscriber(uri, String.class, String.class, subscriber);

        ref.publish("Message-1");
        ref.publish("Message-2");
        assertEquals("Message-1", values.poll(2, SECONDS));
        assertEquals("Message-2", values.poll(2, SECONDS));
        Jvm.pause(100);
        assertEquals("[]", values.toString());
    }
}

