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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.server.WireType;
import net.openhft.chronicle.engine.tree.*;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static net.openhft.chronicle.engine.server.WireType.wire;
import static org.easymock.EasyMock.verify;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class TopologicalSubscriptionEventTest extends ThreadMonitoringTest {
    private static final String NAME = "test";

    private static Boolean isRemote;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public TopologicalSubscriptionEventTest(Object isRemote, Object wireType) {
        TopologicalSubscriptionEventTest.isRemote = (Boolean) isRemote;

        wire = (Function<Bytes, Wire>) wireType;
    }

    @Parameters
    public static Collection<Object[]> data() throws IOException {
        return Arrays.asList(
                new Object[]{Boolean.FALSE, WireType.TEXT}
                , new Object[]{Boolean.FALSE, WireType.BINARY}
                , new Object[]{Boolean.TRUE, WireType.TEXT}
                , new Object[]{Boolean.TRUE, WireType.BINARY}
        );
    }

    private static void waitFor(Object subscriber) {
        for (int i = 1; i < 10; i++) {
            Jvm.pause(i);
            try {
                verify(subscriber);
            } catch (AssertionError e) {
                // retry
            }
        }
        verify(subscriber);
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());

            serverEndpoint = new ServerEndpoint(serverAssetTree);

            assetTree = new VanillaAssetTree().forRemoteAccess("localhost", serverEndpoint.getPort());
        } else
            assetTree = serverAssetTree;


    }

    @After
    public void after() throws IOException {
        assetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();

    }



    @Test
    public void testTopologicalEvents() throws IOException, InterruptedException {

        final BlockingQueue<TopologicalEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to new maps being added and removed.";
                Subscriber<TopologicalEvent> subscription = eventsQueue::add;
                serverAssetTree.registerSubscriber("", TopologicalEvent.class, subscription);

                {
                    TopologicalEvent take = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(ExistingAssetEvent.of(null, ""), take);
                }
                {
                    assetTree.acquireMap("/group/" + NAME, String.class, String.class).size();

                    TopologicalEvent take1 = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(AddedAssetEvent.of("/", "group"), take1);

                    TopologicalEvent take2 = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(AddedAssetEvent.of("/group", NAME), take2);

                }
                {
                    serverAssetTree.acquireMap("/group/" + NAME + 2, String.class, String.class);

                    TopologicalEvent take3 = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(AddedAssetEvent.of("/group", NAME + 2), take3);
                }
                {
                    // the client cannot remove maps yet.
                    serverAssetTree.acquireAsset(RequestContext.requestContext("/group")).removeChild(NAME);

                    TopologicalEvent take4 = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(RemovedAssetEvent.of("/group", NAME), take4);
                }

                assetTree.unregisterSubscriber(NAME, subscription);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }


}



