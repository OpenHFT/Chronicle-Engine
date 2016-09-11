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

    private final Boolean isRemote;
    private final WireType wireType;
    public String connection = "QueryableTest.host.port";
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;
    public QueryableKeySetTest(boolean isRemote, WireType wireType) {
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

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {
            methodName(name.getMethodName());
            connection = "QueryableKeySetTest.host.port";
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType);
        } else {
            assetTree = serverAssetTree;
        }
    }

    @After
    public void preAfter() {
        assetTree.close();
        Jvm.pause(100);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test(timeout = 10000)
    public void testQueryForEach() {

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
    public void testQueryForEachWithPredicate() {

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
    public void testQueryForCollect() {

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
    public void testForEach() {

        final MapView<Integer, Integer> map = assetTree.acquireMap("name", Integer.class, Integer
                .class);

        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        final Query<Integer> query = map.keySet().query();
        query.filter((obj) -> obj >= 1 && obj <= 2).forEach(System.out::println);

    }
}

