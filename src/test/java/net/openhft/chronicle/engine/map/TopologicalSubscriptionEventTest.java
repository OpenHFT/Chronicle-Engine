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
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.*;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
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

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class TopologicalSubscriptionEventTest extends ThreadMonitoringTest {
    private static final String NAME = "test";

    private static Boolean isRemote;
    private final Function<Bytes, Wire> wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree clientAssetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public TopologicalSubscriptionEventTest(Object isRemote, Function<Bytes, Wire> wireType) {
        TopologicalSubscriptionEventTest.isRemote = (Boolean) isRemote;
        this.wireType = wireType;
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

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());
            TCPRegistry.createServerSocketChannelFor("TopologicalSubscriptionEventTest.host.port");
            serverEndpoint = new ServerEndpoint("TopologicalSubscriptionEventTest.host.port", serverAssetTree, wireType);

            clientAssetTree = new VanillaAssetTree().forRemoteAccess("TopologicalSubscriptionEventTest.host.port", wireType);
        } else
            clientAssetTree = serverAssetTree;


    }

    @After
    public void after() throws IOException {
        clientAssetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();

        TCPRegistry.assertAllServersStopped();
    }



    @Test
    public void testTopologicalEvents() throws IOException, InterruptedException {

        final BlockingQueue<TopologicalEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to new maps being added and removed.";
                Subscriber<TopologicalEvent> subscription = eventsQueue::add;
                clientAssetTree.registerSubscriber("", TopologicalEvent.class, subscription);

                {
                    TopologicalEvent take = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(ExistingAssetEvent.of(null, ""), take);
                }
                {
                    clientAssetTree.acquireMap("/group/" + NAME, String.class, String.class).size();

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
                    serverAssetTree.acquireAsset("/group").removeChild(NAME);

                    TopologicalEvent take4 = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(RemovedAssetEvent.of("/group", NAME), take4);
                }

                clientAssetTree.unregisterSubscriber(NAME, subscription);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }


}



