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
import net.openhft.chronicle.core.onoes.ExceptionKey;
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
import java.util.Map;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
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
public class ArrayMapTest extends ThreadMonitoringTest {
    private static final String NAME = "test";
    private static MapView<String, byte[]> map;

    private final Boolean isRemote;
    private final WireType wireType;
    public String connection;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;
    private Map<ExceptionKey, Integer> exceptions;

    public ArrayMapTest(boolean isRemote, WireType wireType) {
        this.isRemote = isRemote;
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, null}
                , new Object[]{true, WireType.TEXT}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
                , new Object[]{true, WireType.BINARY}
        );
    }

    @Before
    public void before() throws IOException {
        exceptions = Jvm.recordExceptions();
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());
            connection = "ArrayMapTest." + name.getMethodName() + ".host.port" + wireType;
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType);
        } else {
            assetTree = serverAssetTree;
        }

        map = assetTree.acquireMap(NAME, String.class, byte[].class);
    }

    @After
    public void preAfter() {

        serverAssetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();

        assetTree.close();

        closeQuietly(map);
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test
    public void testByteArrayValue() {

        final MapView<String, byte[]> map = assetTree.acquireMap("name", String.class, byte[]
                .class);
        map.put("1", "hello world".getBytes(ISO_8859_1));

        final byte[] bytes = map.get("1");
        Assert.assertArrayEquals("hello world".getBytes(ISO_8859_1), bytes);

    }

    @Test
    public void testByteArrayValueWithRealBytes() {

        final MapView<String, byte[]> map = assetTree.acquireMap("name", String.class, byte[]
                .class);

        final byte[] expected = {1, 2, 3, 4, 5, 6, 7};
        map.put("1", expected);

        final byte[] actual = map.get("1");
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testByteArrayValueWithRealBytesNegitive() {

        final MapView<String, byte[]> map = assetTree.acquireMap("name", String.class, byte[]
                .class);

        final byte[] expected = {-1, -2, -3, -4, -5, -6, -7};
        map.put("1", expected);

        final byte[] actual = map.get("1");
        Assert.assertArrayEquals(expected, actual);
    }
}

