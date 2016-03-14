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
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */

public class TcpManyClientConnectionsTest extends ThreadMonitoringTest {

    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final int MAX = 50;
    private static final String NAME = "test";
    private static final String CONNECTION = "host.port.TcpManyConnectionsTest";
    private static ConcurrentMap[] maps = new ConcurrentMap[MAX];
    private static AtomicReference<Throwable> t = new AtomicReference();
    private AssetTree[] trees = new AssetTree[MAX];
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Before
    public void before() throws IOException {
        YamlLogging.setAll(false);
        serverAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));

        TCPRegistry.createServerSocketChannelFor(CONNECTION);

        serverEndpoint = new ServerEndpoint(CONNECTION, serverAssetTree);

        for (int i = 0; i < MAX; i++) {
            trees[i] = new VanillaAssetTree().forRemoteAccess(CONNECTION, WIRE_TYPE, x -> t.set(x));
            maps[i] = trees[i].acquireMap(NAME, String.class, String.class);
        }
    }

    @After
    public void after() throws IOException {

        shutdownTrees();

        serverAssetTree.close();
        serverEndpoint.close();

        TcpChannelHub.closeAllHubs();
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

        final ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i = 0; i < MAX; i++) {
            final int j = i;
            executorService.submit(() -> {
                maps[j].put("hello" + j, "world" + j);
                Assert.assertEquals("world" + j, maps[j].get("hello" + j));

            });

        }
    }

}

