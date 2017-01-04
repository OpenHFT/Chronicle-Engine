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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;

/**
 * @author Rob Austin.
 */

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class RemoteSubscriptionTest extends ThreadMonitoringTest {

    private static final String NAME = "test";
    private static MapView<String, String> map;

    private final WireType wireType;
    public String connection;
    @NotNull
    @Rule
    public TestName name = new TestName();
    @NotNull
    private AssetTree clientAssetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public RemoteSubscriptionTest(WireType wireType) {
        this.wireType = wireType;
    }

    @NotNull
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        @NotNull final List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{WireType.BINARY});
        list.add(new Object[]{WireType.TEXT});
        return list;
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        methodName(name.getMethodName());

//        YamlLogging.showServerWrites(true);
//        YamlLogging.showServerReads(true);

        connection = "StreamTest." + name.getMethodName() + ".host.port" + wireType;
        TCPRegistry.createServerSocketChannelFor(connection);
        serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
        clientAssetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType);

        map = clientAssetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void preAfter() {
        clientAssetTree.close();
        Jvm.pause(100);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        net.openhft.chronicle.core.io.Closeable.closeQuietly(map);

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    /**
     * doing a put on the server, listening for the event on the client
     *
     * @throws Exception
     */
    @Test
    public void putServerListenOnClient() throws Exception {

        @NotNull final MapView<String, String> serverMap = serverAssetTree.acquireMap("name", String.class, String
                .class);

        @NotNull final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(1);
        clientAssetTree.registerSubscriber("name", MapEvent.class, events::add);

        serverMap.put("hello", "world");

        final MapEvent event = events.poll(10, SECONDS);

        Assert.assertTrue(event instanceof InsertedEvent);

    }

    /**
     * doing a put on the client, listening for the event on the server
     *
     * @throws Exception
     */
    @Test
    public void putClientListenOnServer() throws Exception {

        @NotNull final MapView<String, String> clientMap = clientAssetTree.acquireMap("name", String.class, String
                .class);

        @NotNull final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(1);
        serverAssetTree.registerSubscriber("name", MapEvent.class, events::add);

        clientMap.put("hello", "world");

        final MapEvent event = events.poll(10, SECONDS);

        Assert.assertTrue(event instanceof InsertedEvent);

    }

    /**
     * doing a put on the client, listening for the event on the server
     *
     * @throws Exception
     */
    @Test
    public void putClientListenOnClient() throws Exception {

        @NotNull final MapView<String, String> clientMap = clientAssetTree.acquireMap("name", String.class, String
                .class);

        @NotNull final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(1);
        clientAssetTree.registerSubscriber("name?putReturnsNull=true", MapEvent.class,
                events::add);
        {
            clientMap.put("hello", "world");

            final MapEvent event = events.poll(10, SECONDS);

            Assert.assertTrue(event instanceof InsertedEvent);
        }
        {
            clientMap.put("hello", "world2");

            final MapEvent event = events.poll(10, SECONDS);

            Assert.assertTrue(event instanceof UpdatedEvent);
        }
    }

    /**
     * doing a put on the client, listening for the event on the server
     *
     * @throws Exception
     */
    @Test
    public void testEndOfSubscription() throws InterruptedException {

        @NotNull BlockingQueue<Boolean> endSub = new ArrayBlockingQueue<>(1);

        @NotNull final Subscriber<MapEvent> eventHandler = new Subscriber<MapEvent>() {

            @Override
            public void onMessage(MapEvent mapEvent) {
                // do nothing
            }

            @Override
            public void onEndOfSubscription() {
                endSub.add(Boolean.TRUE);
            }
        };

        Jvm.pause(100);
        clientAssetTree.registerSubscriber("name", MapEvent.class, eventHandler);
        Jvm.pause(100);
        clientAssetTree.unregisterSubscriber("name", eventHandler);

        final Boolean onEndOfSubscription = endSub.poll(20, SECONDS);

        Assert.assertTrue(onEndOfSubscription);
    }
}

