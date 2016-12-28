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
import net.openhft.chronicle.engine.Utils;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.set.EntrySetView;
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
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
public class QueryableEntrySetTest extends ThreadMonitoringTest {

    private static final String NAME = "test";
    private static MapView<String, String> map;
    @NotNull

    private final Boolean isRemote;
    private final WireType wireType;
    @NotNull
    public String connection = "QueryableTest.host.port";
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;
    public QueryableEntrySetTest(boolean isRemote, WireType wireType) {
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

//            YamlLogging.showServerWrites(true);
//            YamlLogging.showServerReads(true);

            connection = "StreamTest." + name.getMethodName() + ".host.port";
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType);
        } else {
            assetTree = serverAssetTree;
        }

        map = assetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void preAfter() {
        assetTree.close();
        Jvm.pause(100);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        net.openhft.chronicle.core.io.Closeable.closeQuietly(map);
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test(timeout = 10000)
    public void testQueryForEach() {

        @NotNull Map<String, String> expected = new HashMap<>();
        expected.put("1", "1");
        expected.put("2", "2");

        @NotNull final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);
        map.putAll(expected);
        Utils.waitFor(() -> map.size() == expected.size());

        @NotNull final EntrySetView<String, Object, String> query = map.entrySet();
        query.query();
        @NotNull final Set<Map.Entry<String, String>> actual = new HashSet<>();
        query.forEach(actual::add);

        System.out.println(actual);
        Assert.assertEquals(expected.entrySet(), actual);
    }

    @Test(timeout = 10000)
    public void testQueryForEachWithPredicate() throws InterruptedException {

        @NotNull final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");

//        YamlLogging.showServerReads(true);
//        YamlLogging.showServerWrites(true);

        @NotNull final EntrySetView<String, Object, String> entries = map.entrySet();
        @NotNull final Query<Map.Entry<String, String>> query = entries.query();
        @NotNull final BlockingQueue<Map.Entry> result = new ArrayBlockingQueue<>(1);
        final Consumer<Map.Entry<String, String>> consumer = result::add;

        query.filter(o -> "1".equals(o.getKey()) && "1".equals(o.getValue())).forEach(consumer);

        final Map.Entry entry = result.poll(10, TimeUnit.SECONDS);

        Assert.assertEquals("1", entry.getKey());
        Assert.assertEquals("1", entry.getValue());

    }
}

