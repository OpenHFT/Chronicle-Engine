/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.Utils.yamlLoggger;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
public class ThrottledKeySubscriptionEventTest extends ThreadMonitoringTest {

    public static final WireType WIRE_TYPE = WireType.TEXT;
    private static final String NAME = "test";
    private static MapView<String, String> map;

    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    @Before
    public void before() throws IOException {
        System.setProperty("Throttler.maxEventsPreSecond", "1");
        serverAssetTree = new VanillaAssetTree().forTesting();

        methodName(name.getMethodName());
        final String hostPort = "ThrottledKeySubscriptionEventTest." + name.getMethodName() + ".host.port";
        TCPRegistry.createServerSocketChannelFor(hostPort);

        serverEndpoint = new ServerEndpoint(hostPort, serverAssetTree, WIRE_TYPE);
        assetTree = new VanillaAssetTree().forRemoteAccess(hostPort, WIRE_TYPE);

        map = assetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void after() throws IOException {
        assetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        if (map instanceof Closeable)
            ((Closeable) map).close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        System.setProperty("Throttler.maxEventsPreSecond", "0");
    }


    /**
     * because we have set System.setProperty("Throttler.maxEventsPreSecond", "1"); in the static
     * above, we will only get one event per second, this test also checks that the messages still
     * arrive in order.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    @Ignore("todo fix")
    public void testReceivingThrottledEventsInOrder() throws IOException, InterruptedException {

        final BlockingQueue<String> eventsQueue = new LinkedBlockingDeque<>();

        YamlLogging.showServerWrites = true;
        YamlLogging.showServerReads = true;

        yamlLoggger(() -> {
            try {

                Subscriber<String> add = eventsQueue::add;
                assetTree.registerSubscriber(NAME, String.class, add);

                for (int i = 0; i < 10; i++) {
                    map.put("Hello" + i, "World" + i);
                }

                final long start = System.currentTimeMillis();

                for (int i = 0; i < 10; i++) {
                    String actual = eventsQueue.poll(5, SECONDS);
                    Assert.assertNotNull(actual);
                    Assert.assertEquals("Hello" + i, actual);
                }

                // because we are only sending 1 message per second this should take around 10
                // seconds, certainly longer than 5 seconds
                Assert.assertTrue(System.currentTimeMillis() > start + TimeUnit.SECONDS.toMillis(5));

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });

    }


}





