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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */

public class KeySubscriptionTest extends ThreadMonitoringTest {

    public static final WireType WIRE_TYPE = WireType.TEXT;
    private static final String NAME = "test";
    private static final String CONNECTION = "localhost:8080";

    private AssetTree clientTree;
    private VanillaAssetTree serverAssetTree;
    //   private ServerEndpoint serverEndpoint;

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        //TCPRegistry.createServerSocketChannelFor(CONNECTION);
        //     serverEndpoint = new ServerEndpoint(CONNECTION, serverAssetTree);
        clientTree = new VanillaAssetTree().forRemoteAccess(CONNECTION, WIRE_TYPE);
    }

    public void preAfter() {

        clientTree.close();

        serverAssetTree.close();
        //    serverEndpoint.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    /**
     * test many clients connecting to a single server
     */
    @Test
    public void test() throws IOException, InterruptedException {

        final MapView<String, String> serverMap = serverAssetTree.acquireMap(NAME, String
                .class, String.class);

        serverMap.put("hello", "world");

        final MapView<String, String> map = clientTree.acquireMap(NAME, String.class,
                String.class);

        map.registerKeySubscriber(System.out::println);


    }

    /**
     * test registerKeySubscriber before doing an operation ont the map
     */
    @Test(timeout = 10000)
    public void testKey() throws IOException, InterruptedException {

        BlockingQueue<String> q = new ArrayBlockingQueue<>(1);

        clientTree.acquireMap(NAME, String.class,
                String.class).registerKeySubscriber(q::add);

        Jvm.pause(500);

        final MapView<String, String> serverMap = serverAssetTree.acquireMap(NAME,
                String.class, String.class);

        serverMap.put("hello", "world");
        Assert.assertEquals("hello", q.poll(10, TimeUnit.SECONDS));

    }

    @Test(timeout = 10000)
    public void testSubscriptionOnKey() throws InterruptedException {

        //Enable Yaml logging when running in debug.

        YamlLogging.setAll(false);
        String key = "key";
        String keyUri = NAME + "/" + key + "?bootstrap=false";

        BlockingQueue<String> q = new ArrayBlockingQueue<>(2);

        final MapView<String, String> server = clientTree.acquireMap(NAME, String.class,
                String.class);

        // we have to call an action on the server map because it lazily created
        server.size();

        Jvm.pause(500);

        clientTree.registerSubscriber(keyUri, String.class, q::add);

        Jvm.pause(500);

        server.put(key, "val1");
        server.put(key, "val2");

        Assert.assertEquals("val1", q.poll(10, TimeUnit.SECONDS));
        Assert.assertEquals("val2", q.poll(10, TimeUnit.SECONDS));
    }
}

