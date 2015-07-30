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

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */

public class TcpManyClientConnectionsTest extends ThreadMonitoringTest {

    private static final String NAME = "test";
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final int MAX = 100;
    private static ConcurrentMap[] maps = new ConcurrentMap[MAX];
    private static final String CONNECTION = "host.port.TcpManyConnectionsTest";
    private AssetTree[] trees = new AssetTree[MAX];
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;


    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        TCPRegistry.createServerSocketChannelFor(CONNECTION);

        serverEndpoint = new ServerEndpoint(CONNECTION, serverAssetTree, WIRE_TYPE);

        for (int i = 0; i < MAX; i++) {
            trees[i] = new VanillaAssetTree().forRemoteAccess(CONNECTION, WIRE_TYPE);
            maps[i] = trees[i].acquireMap(NAME, String.class, String.class);
        }

    }

    @After
    public void after() throws IOException {

        shutdownTrees();

        serverAssetTree.close();
        serverEndpoint.close();

        TCPRegistry.reset();
    }

    private void shutdownTrees() {
        final ArrayList<Future> futures = new ArrayList<>();

        ExecutorService c = Executors.newCachedThreadPool(new NamedThreadFactory("Tree Closer",
                true));
        for (int i = 1; i < MAX; i++) {
            final int j = i;
            futures.add(c.submit(trees[j]::close));

        }

        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    /**
     * test many clients connecting to a single server
     */
    @Test
    public void test() throws IOException, InterruptedException {

        final MapView<String, String, String> serverMap = serverAssetTree.acquireMap(NAME, String
                .class, String.class);

        serverMap.put("hello", "world");

        for (int i = 0; i < MAX; i++) {
            Assert.assertEquals("world", maps[i].get("hello"));
        }

    }


}




