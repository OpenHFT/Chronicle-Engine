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
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    @Before
    public void before() throws IOException {
        methodName(name.getMethodName());
        methodName = name.getMethodName();
        assetTree = (new VanillaAssetTree(1)).forTesting();
        YamlLogging.setAll(false);
    }

    public void preAfter() {
        methodName = "";
    }

    @Test
    @Ignore("TODO FIX too many results")
    public void test() throws InterruptedException {
        @NotNull String uri = "/queue/" + methodName;

        @NotNull final Reference<String> ref = assetTree.acquireReference(uri, String.class);
        @NotNull BlockingQueue<String> values = new LinkedBlockingQueue<>();
        @Nullable Subscriber<String> subscriber = e -> {
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
        @NotNull String uri = "/queue/" + methodName;
        assetTree.acquireQueue(uri, String.class, String.class);
        @NotNull final Reference<String> ref = assetTree.acquireReference(uri + "/key", String.class);
        @NotNull BlockingQueue<String> values = new LinkedBlockingQueue<>();
        @Nullable TopicSubscriber<String, String> subscriber = (topic, message) -> {
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

