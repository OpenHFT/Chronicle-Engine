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

import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapView;
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
import java.util.Arrays;
import java.util.Collection;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;
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
public class ServerOverloadTest extends ThreadMonitoringTest {
    public static final int SIZE = 100;
    private static MapView<String, String> map;
    private final Boolean isRemote;
    private final WireType wireType;
    public String connection = "QueryableTest.host.port";
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public ServerOverloadTest(boolean isRemote, WireType wireType) {
        this.isRemote = isRemote;
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, null},
                new Object[]{true, WireType.BINARY},
                new Object[]{true, WireType.TEXT}
        );
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());
            connection = "ServerOverloadTest.testThatSendingAlotOfDataToTheServer.host.port";
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType);
        } else {
            assetTree = serverAssetTree;
        }
    }

    @After
    public void preAfter() {
        closeQuietly(map);
        assetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        if (serverAssetTree != assetTree)
            serverAssetTree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test
    public void testThatSendingAlotOfDataToTheServer() {
//        YamlLogging.setAll(true);
        map = assetTree.acquireMap("name", String.class, String.class);

        //
        char[] largeChar = new char[66000];

        Arrays.fill(largeChar, 'X');

        final String large2MbString = new String(largeChar);

        for (int i = 0; i < SIZE; i++) {
//            System.out.println("i: " + i);
    //        Assert.assertEquals(i, map.size());
            map.put("" + i, large2MbString);
        }
        System.out.println("gets here");
        Assert.assertEquals(SIZE, map.size());
    }
}

