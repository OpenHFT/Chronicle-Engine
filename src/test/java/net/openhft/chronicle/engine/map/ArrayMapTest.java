/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

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
    private static AtomicReference<Throwable> t = new AtomicReference();
    private final Boolean isRemote;
    private final WireType wireType;
    public String connection;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

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
        );
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));

        if (isRemote) {

            methodName(name.getMethodName());
            connection = "ArrayMapTest." + name.getMethodName() + ".host.port" + wireType;
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType, x -> t.set(x));
        } else {
            assetTree = serverAssetTree;
        }

        map = assetTree.acquireMap(NAME, String.class, byte[].class);
        YamlLogging.setAll(true);
    }

    @After
    public void after() throws IOException {

        serverAssetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();

        assetTree.close();

        if (map instanceof Closeable)
            ((Closeable) map).close();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }


    @Test
    public void testByteArrayValue() throws Exception {

        final MapView<String, byte[]> map = assetTree.acquireMap("name", String.class, byte[]
                .class);
        map.put("1", "hello world".getBytes());

        final byte[] bytes = map.get("1");
        Assert.assertArrayEquals("hello world".getBytes(), bytes);

    }


    @Test
    public void testByteArrayValueWithRealBytes() throws Exception {

        final MapView<String, byte[]> map = assetTree.acquireMap("name", String.class, byte[]
                .class);

        final byte[] expected = {1, 2, 3, 4, 5, 6, 7};
        map.put("1", expected);

        final byte[] actual = map.get("1");
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testByteArrayValueWithRealBytesNegitive() throws Exception {

        final MapView<String, byte[]> map = assetTree.acquireMap("name", String.class, byte[]
                .class);

        final byte[] expected = {-1, -2, -3, -4, -5, -6, -7};
        map.put("1", expected);

        final byte[] actual = map.get("1");
        Assert.assertArrayEquals(expected, actual);
    }


}

