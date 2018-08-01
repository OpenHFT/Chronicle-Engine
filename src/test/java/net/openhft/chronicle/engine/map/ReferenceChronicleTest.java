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
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.ShutdownHooks;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ReferenceChronicleTest {

    @Rule
    public ShutdownHooks hooks = new ShutdownHooks();
    private String hostPortToken;

    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptions;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @Before
    public void before() throws IOException {
        hostPortToken = this.getClass().getSimpleName() + ".host.port";
        TCPRegistry.createServerSocketChannelFor(hostPortToken);
        exceptions = Jvm.recordExceptions();
    }

    @After
    public void after() {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @After
    public void afterMethod() {
        ThreadMonitoringTest.filterExceptions(exceptions);
        if (Jvm.hasException(exceptions)) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @After
    public void checkThreadDump() {
        threadDump.ignore("FailOnTimeoutGroup/ChronicleMapKeyValueStore Closer");
        threadDump.assertNoNewThreads();
    }

    @Test(timeout = 5000)
    public void testRemoteSubscriptionMUFGChronicle() throws IOException {

        @NotNull AssetTree serverAssetTree = hooks.addCloseable(new VanillaAssetTree().forTesting());
        serverAssetTree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore", VanillaMapView::new, KeyValueStore.class);
        serverAssetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.basePath(OS.TARGET).entries(50).averageValueSize(2_000_000), asset));

        @NotNull ServerEndpoint serverEndpoint = hooks.addCloseable(new ServerEndpoint(hostPortToken, serverAssetTree, "cluster"));
        @NotNull AssetTree clientAssetTree = hooks.addCloseable(new VanillaAssetTree().forRemoteAccess(hostPortToken, WireType.BINARY));

        //noinspection TryFinallyCanBeTryWithResources
        try {
            test(clientAssetTree);
        } finally {
            clientAssetTree.close();
            serverEndpoint.close();
            serverAssetTree.close();
        }
    }

    @Test(timeout = 5000)
    public void testLocalSubscriptionMUFGChronicle() {

        @NotNull AssetTree serverAssetTree = hooks.addCloseable(new VanillaAssetTree().forTesting());
        serverAssetTree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore", VanillaMapView::new, KeyValueStore.class);
        serverAssetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.basePath(OS.TARGET).entries(50).averageValueSize(2_000_000), asset));
        test(serverAssetTree);
        serverAssetTree.close();

    }

    public void test(@NotNull AssetTree assetTree) {
        @NotNull String key = "subject";
        @NotNull String _mapName = "group";
        @NotNull Map map = assetTree.acquireMap(_mapName, String.class, String.class);
        //TODO does not work without an initial put
        map.put(key, "init");

        @NotNull List<String> events = new ArrayList<>();
        @NotNull Subscriber<String> keyEventSubscriber = new Subscriber<String>() {
            @Override
            public void onMessage(String s) {
                events.add(s);
            }

            @Override
            public void onEndOfSubscription() {
                events.add("END");
            }
        };

        @NotNull TopicSubscriber<String, String> topicSubscriber = (t, m) -> {
            events.add(m);
        };

        assetTree.registerSubscriber(_mapName + "/" + key + "?bootstrap=false&putReturnsNull=true", String.class, keyEventSubscriber);
        //serverAssetTree.registerTopicSubscriber(_mapName + "?bootstrap=false&putReturnsNull=true", String.class, String.class, topicSubscriber);

        Jvm.pause(100);
        Asset child = assetTree.getAsset(_mapName).getChild(key);
        //Asset child = serverAssetTree.getAsset(_mapName);

        assertNotNull(child);
        @Nullable SubscriptionCollection subscription = child.subscription(false);
        assertEquals(1, subscription.subscriberCount());

//        YamlLogging.showServerWrites(true);

        @NotNull AtomicInteger count = new AtomicInteger();
        map.put(key, "" + count.incrementAndGet());
        map.put(key, "" + count.incrementAndGet());
        map.put(key, "" + count.incrementAndGet());

        while (events.size() != 3) {
            Jvm.pause(1);
        }

        assetTree.unregisterSubscriber(_mapName + "/" + key, keyEventSubscriber);
        //serverAssetTree.unregisterTopicSubscriber(_mapName, topicSubscriber);
        while (events.size() != 4) {
            Jvm.pause(1);
        }

        //assertEquals(0, subscription.subscriberCount());
        assertEquals(4, events.size());
    }
}
