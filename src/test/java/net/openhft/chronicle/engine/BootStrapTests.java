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
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */


@RunWith(Parameterized.class)
public class BootStrapTests {


    public static final int MAX = 10;
    public static final String CLUSTER_NAME = "max-cluster";

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[5][0]);
    }

    public BootStrapTests() {
    }

    private static final String NAME = "test";
    public static final WireType WIRE_TYPE = WireType.TEXT;
    private static ConcurrentMap<String, String> map1, map2;


    private static final String CONNECTION_1 = "Test1.host.port";
    private final static String CONNECTION_2 = "Test2.host.port";

    private AssetTree client1;
    private AssetTree client2;


    private VanillaAssetTree serverAssetTree1;

    private ServerEndpoint serverEndpoint1;


    @Before
    public void before() throws IOException {
        serverAssetTree1 = new VanillaAssetTree().forTesting();

        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);
        TCPRegistry.createServerSocketChannelFor(CONNECTION_2);

        serverEndpoint1 = new ServerEndpoint(CONNECTION_1, serverAssetTree1, WIRE_TYPE);

        client1 = new VanillaAssetTree("client1").forRemoteAccess
                (CONNECTION_1, WIRE_TYPE);

        client2 = new VanillaAssetTree("client2").forRemoteAccess
                (CONNECTION_1, WIRE_TYPE);


    }

    @After
    public void after() throws IOException {
        client1.close();
        client2.close();

        if (serverEndpoint1 != null)
            serverEndpoint1.close();

        serverAssetTree1.close();

        if (map1 instanceof Closeable)
            ((Closeable) map1).close();

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
    public void nonBootstrappingTest() throws IOException, InterruptedException {

        try {

            {
                map1 = client1.acquireMap(NAME, String.class, String.class);
                Queue q1 = new ConcurrentLinkedQueue();
                client1.registerSubscriber(NAME, MapEvent.class, q1::add);


                map1.put("hello", "world1");

                Assert.assertEquals("world1", map1.get("hello"));
                final String poll = q1.poll().toString();
                Assert.assertEquals("InsertedEvent{assetName='/test', key=hello, value=world1}",
                        poll);
            }

            client1.close();

            {
                map2 = client2.acquireMap(NAME, String.class, String.class);
                Queue q2 = new ConcurrentLinkedQueue();
                client2.registerSubscriber(NAME + "?bootstrap=false", MapEvent.class, q2::add);

                map2.put("hello", "world2");
                // shutting server1 down should cause the failover client to connect to server 2
                Assert.assertEquals("world2", map2.get("hello"));
                final String poll = q2.poll().toString();
                Assert.assertEquals("UpdatedEvent{assetName='/test', key=hello, oldValue=world1, value=world2}", poll);
            }

        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }

    }


    /**
     * the fail over client connects to  server1 ( server1 is the primary) , server1 is then shut
     * down and the client connects to the secondary
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void bootstrappingTest() throws IOException, InterruptedException {

        try {

            {
                map1 = client1.acquireMap(NAME, String.class, String.class);
                Queue q1 = new ConcurrentLinkedQueue();
                client1.registerSubscriber(NAME, MapEvent.class, q1::add);


                map1.put("hello", "world1");

                Assert.assertEquals("world1", map1.get("hello"));
                final String poll = q1.poll().toString();
                Assert.assertEquals("InsertedEvent{assetName='/test', key=hello, value=world1}",
                        poll);
            }

            client1.close();

            {
                map2 = client2.acquireMap(NAME, String.class, String.class);
                Queue q2 = new ConcurrentLinkedQueue();
                client2.registerSubscriber(NAME + "?bootstrap=false", MapEvent.class, q2::add);

                map2.put("hello", "world2");
                // shutting server1 down should cause the failover client to connect to server 2
                Assert.assertEquals("world2", map2.get("hello"));
                final String poll = q2.poll().toString();
                Assert.assertEquals("UpdatedEvent{assetName='/test', key=hello, oldValue=world1, value=world2}", poll);
            }

        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }

    }


}




