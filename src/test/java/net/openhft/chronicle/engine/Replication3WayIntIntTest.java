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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */
@RunWith(value = Parameterized.class)
public class Replication3WayIntIntTest extends ThreadMonitoringTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final int NUMBER_OF_TIMES = 10;

    static {
        System.setProperty("ReplicationHandler3", "true");
    }

    public ServerEndpoint serverEndpoint1;
    public ServerEndpoint serverEndpoint2;
    @Rule
    public TestName testName = new TestName();
    public String name;
    private ServerEndpoint serverEndpoint3;
    private AssetTree tree1;
    private AssetTree tree2;
    private AssetTree tree3;
    private AtomicReference<Throwable> t = new AtomicReference<>();

    public Replication3WayIntIntTest() {

    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] objects = new Object[NUMBER_OF_TIMES][];
        Arrays.fill(objects, new Object[]{});
        return Arrays.asList(objects);
    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    @Before
    public void before() throws IOException {
        YamlLogging.setAll(false);

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run

        TCPRegistry.createServerSocketChannelFor(
                "host.port1",
                "host.port2",
                "host.port3");

        WireType writeType = WireType.TEXT;
        tree1 = create(1, writeType, "clusterThree");
        tree2 = create(2, writeType, "clusterThree");
        tree3 = create(3, writeType, "clusterThree");

        serverEndpoint1 = new ServerEndpoint("host.port1", tree1);
        serverEndpoint2 = new ServerEndpoint("host.port2", tree2);
        serverEndpoint3 = new ServerEndpoint("host.port3", tree3);

        name = testName.getMethodName();

        Files.deleteIfExists(Paths.get(OS.TARGET, name));
    }

    public void preAfter() {
        Closeable.closeQuietly(serverEndpoint1);
        Closeable.closeQuietly(serverEndpoint2);
        Closeable.closeQuietly(serverEndpoint3);

        Closeable.closeQuietly(tree1);
        Closeable.closeQuietly(tree2);
        Closeable.closeQuietly(tree3);

    }

    @NotNull
    private AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x))
                .withConfig(resourcesDir() + "/3way", OS.TARGET + "/" + hostId);

        tree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).cluster(clusterTwo),
                        asset));

        return tree;
    }

    @Test
    public void testAllDataGetsReplicated() {

        name = "testAllDataGetsReplicated";
        final ConcurrentMap<Integer, Integer> map1 = tree1.acquireMap(name, Integer.class, Integer
                .class);
        assertNotNull(map1);

        final ConcurrentMap<Integer, Integer> map2 = tree2.acquireMap(name, Integer.class, Integer
                .class);
        assertNotNull(map2);

        final ConcurrentMap<Integer, Integer> map3 = tree3.acquireMap(name, Integer.class, Integer
                .class);
        assertNotNull(map3);

        for (int i = 0; i < 70; i++) {
            map1.put(i, i);
        }

        assertFullRange(70, map1);

        Jvm.pause(1);

        assertFullRange(70, map2);

        put(map2, 70);

        assertFullRange(71, map1, map2);

        assertFullRange(71, map3);

        assertAllContain(70, map1, map2, map3);

        put(map1, 71);
        assertAllContain(71, map1, map2, map3);

        put(map2, 72);
        assertAllContain(72, map1, map2, map3);

        put(map3, 73);
        assertAllContain(73, map1, map2, map3);

        // you have to wait for it to become fully replicated

        int epectedSize = 74;

        for (int i = 0; i < 100; i++) {
            if (map1.size() == epectedSize &&
                    map2.size() == epectedSize &&
                    map3.size() == epectedSize) {
                break;
            }
            Jvm.pause(100);
        }

        if (map1.size() != map2.size())
            System.out.println("map1=" + map1 + "\nmap2=" + map2);

        Assert.assertEquals(map1.size(), map2.size());
        Assert.assertEquals(map2.size(), map3.size());

    }

    private void put(Map<Integer, Integer> map, int toPut) {
        map.put(toPut, toPut);
    }

    @SafeVarargs
    private final void assertAllContain(int key, Map<Integer, Integer>... maps) {
        Outer:
        for (int x = 0; x < 100; x++) {
            for (Map<Integer, Integer> map : maps) {
                if (!map.containsKey(key)) {
                    Jvm.pause(100);
                    continue Outer;
                }
                assert true;
                return;
            }
        }

        System.out.println("key=" + key + ",maps=" + Arrays.toString(maps));
        assert false;
    }

    @SafeVarargs
    private final void assertFullRange(int upperBoundExclusive, Map<Integer, Integer>... maps) {

        Outer:
        for (int x = 0; x < 100; x++) {

            for (Map<Integer, Integer> map : maps) {
                for (int i = 0; i < upperBoundExclusive; i++) {

                    if (!map.containsKey(i)) {
                        Jvm.pause(100);
                        continue Outer;
                    }

                    assert true;
                    return;
                }
            }
        }

        assert false;
    }
}

