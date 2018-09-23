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
import net.openhft.chronicle.engine.ShutdownHooks;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;

@RunWith(value = Parameterized.class)
public class TestInsertUpdateChronicleMapViewOnServer extends ThreadMonitoringTest {

    @NotNull
    public String connection = "RemoteSubscriptionTest.host.port";
    @Rule
    public ShutdownHooks hooks = new ShutdownHooks();
    private AssetTree clientAssetTree;
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;
    private WireType wireType;

    public TestInsertUpdateChronicleMapViewOnServer(WireType wireType) {
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
        serverAssetTree = hooks.addCloseable(new VanillaAssetTree().forTesting());

        YamlLogging.setAll(false);

        connection = "TestInsertUpdateChronicleMapView.host.port";
        TCPRegistry.createServerSocketChannelFor(connection);
        serverEndpoint = hooks.addCloseable(new ServerEndpoint(connection, serverAssetTree, "cluster"));

        serverAssetTree.root().addWrappingRule(MapView.class, "map directly to " + "KeyValueStore",
                VanillaMapView::new, KeyValueStore.class);

        // TODO fix
        /*serverAssetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.basePath(null).entries(100)
                        .putReturnsNull(false), asset));*/

        clientAssetTree = hooks.addCloseable(new VanillaAssetTree().forRemoteAccess(connection, wireType));

    }

    @Override
    public void preAfter() {
        clientAssetTree.close();
        Jvm.pause(100);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();

    }

    @Test
    public void testInsertFollowedByUpdate() throws InterruptedException {

        @NotNull final MapView<String, String> serverMap = serverAssetTree.acquireMap
                ("name?putReturnsNull=false",
                        String.class, String
                                .class);

        @NotNull final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(1);
        clientAssetTree.registerSubscriber("name?putReturnsNull=false", MapEvent.class,
                events::add);

        {
            serverMap.put("hello", "world");
            final MapEvent event = events.poll(10, SECONDS);
            Assert.assertTrue(event instanceof InsertedEvent);
        }
        {
            serverMap.put("hello", "world2");
            final MapEvent event = events.poll(10, SECONDS);
            Assert.assertTrue(event instanceof UpdatedEvent);
        }
    }

    @Test
    public void testInsertFollowedByUpdateWhenPutReturnsNullTrue() throws InterruptedException {

        @NotNull final MapView<String, String> serverMap = serverAssetTree.acquireMap
                ("name?putReturnsNull=true",
                        String.class, String
                                .class);

        @NotNull final BlockingQueue<MapEvent> events = new ArrayBlockingQueue<>(1);
        clientAssetTree.registerSubscriber("name?putReturnsNull=true", MapEvent.class,
                events::add);

        Jvm.pause(500);

        {
            serverMap.put("hello", "world");
            final MapEvent event = events.poll(10, SECONDS);
            Assert.assertTrue(event instanceof InsertedEvent);
        }
        {
            serverMap.put("hello", "world2");
            final MapEvent event = events.poll(10, SECONDS);
            Assert.assertTrue(event instanceof UpdatedEvent);
        }
    }
}
