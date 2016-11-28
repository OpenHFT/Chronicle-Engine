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
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.*;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
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
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    private final boolean isRemote;
    private final WireType wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree clientAssetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;
    public TopologicalSubscriptionEventTest(boolean isRemote, WireType wireType) {
        this.isRemote = isRemote;
        this.wireType = wireType;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, null}
                , new Object[]{true, WireType.TEXT}
                , new Object[]{true, WireType.BINARY}
        );
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {
            methodName(name.getMethodName());
            TCPRegistry.createServerSocketChannelFor("TopologicalSubscriptionEventTest.host.port");
            serverEndpoint = new ServerEndpoint("TopologicalSubscriptionEventTest.host.port", serverAssetTree);

            clientAssetTree = new VanillaAssetTree().forRemoteAccess("TopologicalSubscriptionEventTest.host.port", wireType);
        } else {
            clientAssetTree = serverAssetTree;
        }
    }

    @After
    public void preAfter() {
        clientAssetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Ignore("Disabled 31/3/16")
    @Test
    public void testTopologicalEvents() throws IOException, InterruptedException {

        final BlockingQueue<TopologicalEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage("Sets up a subscription to listen to new maps being " +
                        "added and removed.");
                Subscriber<TopologicalEvent> subscription = eventsQueue::add;
                clientAssetTree.registerSubscriber("", TopologicalEvent.class, subscription);

                {
                    TopologicalEvent take = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(ExistingAssetEvent.of(null, "", Collections.emptySet()), take);
                }
                {
                    clientAssetTree.acquireMap("/group/" + NAME, String.class, String.class).size();

                    {
                        TopologicalEvent take = eventsQueue.poll(1, SECONDS);
                        Assert.assertEquals(ExistingAssetEvent.of("/", "queue", Collections.emptySet()), take);
                    }
                    {
                        TopologicalEvent take = eventsQueue.poll(1, SECONDS);
                        Assert.assertEquals(AddedAssetEvent.of("/", "group", Collections.emptySet()), take);
                    }
                    {
                        TopologicalEvent take = eventsQueue.poll(1, SECONDS);
                        Assert.assertEquals(AddedAssetEvent.of("/group", NAME, Collections.emptySet()), take);
                    }
                }
                {
                    serverAssetTree.acquireMap("/group/" + NAME + 2, String.class, String.class);

                    TopologicalEvent take3 = eventsQueue.poll(1, SECONDS);

                    Assert.assertEquals(AddedAssetEvent.of("/group", NAME + 2, Collections.emptySet()), take3);
                }
                {
                    // the client cannot remove maps yet.
                    serverAssetTree.acquireAsset("/group").removeChild(NAME);

                    TopologicalEvent take4 = eventsQueue.poll(1, SECONDS);
                    Assert.assertEquals(RemovedAssetEvent.of("/group", NAME, Collections.emptySet()), take4);
                }

                clientAssetTree.unregisterSubscriber(NAME, subscription);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }
}

