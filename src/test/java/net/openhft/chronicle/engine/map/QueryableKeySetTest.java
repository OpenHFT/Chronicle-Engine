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
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.set.KeySetView;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.averagingInt;
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
public class QueryableKeySetTest extends ThreadMonitoringTest {

    private static final String NAME = "test";
    private static AtomicReference<Throwable> t = new AtomicReference();

    private final Boolean isRemote;
    private final WireType wireType;
    public String connection = "QueryableTest.host.port";
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;
    public QueryableKeySetTest(boolean isRemote, WireType wireType) {
        this.isRemote = isRemote;
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
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
            connection = "QueryableKeySetTest.host.port";
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType, x -> t.set(x));
        } else {
            assetTree = serverAssetTree;
        }

        YamlLogging.setAll(true);
    }

    @After
    public void after() throws IOException {
        assetTree.close();
        Jvm.pause(1000);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test(timeout = 10000)
    public void testQueryForEach() throws Exception {

        final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");

        final Query<String> query = map.keySet().query();
        final Set<String> result = new HashSet<>();
        query.forEach(result::add);

        Assert.assertEquals(new HashSet<>(Arrays.asList("1", "2")), result);
    }

    @Test(timeout = 10000)
    public void testQueryForEachWithPredicate() throws Exception {

        final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");

//        YamlLogging.showServerReads = true;
//        YamlLogging.showServerWrites = true;

        final KeySetView<String> remoteSetView = map.keySet();
        final Query<String> query = remoteSetView.query();

        final Set<String> result = new HashSet<>();
        query.filter("1"::equals).forEach(result::add);
        Assert.assertEquals(new HashSet<>(Arrays.asList("1")), result);
    }

    @Test(timeout = 10000)
    public void testQueryForCollect() throws Exception {

        final MapView<Integer, Integer> map = assetTree.acquireMap("name", Integer.class, Integer
                .class);

        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        final Query<Integer> query = map.keySet().query();
        Double x = query.filter((obj) -> obj >= 1 && obj <= 2).collect(averagingInt(v -> (int) v));
        Assert.assertEquals((Double) 1.5, x);
    }

    @Test(timeout = 100000000)
    public void testForEach() throws Exception {

        final MapView<Integer, Integer> map = assetTree.acquireMap("name", Integer.class, Integer
                .class);

        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        final Query<Integer> query = map.keySet().query();
        query.filter((obj) -> obj >= 1 && obj <= 2).forEach(System.out::println);

    }

}

