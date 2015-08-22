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
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */

@Ignore("Doesn't shutdown cleanly")
@RunWith(Parameterized.class)
public class TcpFailoverTest {
    public static final int MAX = 10;
    public static final String CLUSTER_NAME = "max-cluster";
    public static final WireType WIRE_TYPE = WireType.TEXT;
    private static final String NAME = "test";
    private static final String CONNECTION_1 = "Test1.host.port";
    private final static String CONNECTION_2 = "Test2.host.port";
    private static ConcurrentMap<String, String> map;

    private AssetTree failOverClient;
    private VanillaAssetTree serverAssetTree1;
    private VanillaAssetTree serverAssetTree2;
    private ServerEndpoint serverEndpoint1;
    private ServerEndpoint serverEndpoint2;

    public TcpFailoverTest() {
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[10][0]);
    }

    @Before
    public void before() throws IOException {
        serverAssetTree1 = new VanillaAssetTree().forTesting();
        serverAssetTree2 = new VanillaAssetTree().forTesting();

        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);
        TCPRegistry.createServerSocketChannelFor(CONNECTION_2);

        serverEndpoint1 = new ServerEndpoint(CONNECTION_1, serverAssetTree1, WIRE_TYPE);
        serverEndpoint2 = new ServerEndpoint(CONNECTION_2, serverAssetTree2, WIRE_TYPE);

        final String[] connection = {CONNECTION_1, CONNECTION_2};
        failOverClient = new VanillaAssetTree("failoverClient").forRemoteAccess(connection, WIRE_TYPE);

        map = failOverClient.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void after() throws IOException {
        failOverClient.close();

        if (serverEndpoint1 != null)
            serverEndpoint1.close();

        if (serverEndpoint2 != null)
            serverEndpoint2.close();

        serverAssetTree1.close();
        serverAssetTree2.close();

        if (map instanceof Closeable)
            ((Closeable) map).close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    /**
     * the fail over client connects to  server1 ( server1 is the primary) , server1 is then shut
     * down and the client connects to the secondary
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void test() throws IOException, InterruptedException {

        try {

            final MapView<String, String> failoverClient = failOverClient.acquireMap(NAME,
                    String.class,
                    String.class);

            final MapView<String, String> map1 = serverAssetTree1.acquireMap(NAME, String.class,
                    String.class);

            final MapView<String, String> map2 = serverAssetTree2.acquireMap(NAME, String.class,
                    String.class);

            map1.put("hello", "server1");
            map2.put("hello", "server2");

            Assert.assertEquals("server1", failoverClient.get("hello"));

            // we are now going to shut down server 1
            serverAssetTree1.close();

            // shutting server1 down should cause the failover client to connect to server 2
            Assert.assertEquals("server2", failoverClient.get("hello"));

        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }

    }


}




