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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */

public class Replication3WayWithCompressionTest extends ThreadMonitoringTest {

    //   public static final String NAME = "/ChMaps/test";
    private ServerEndpoint serverEndpoint1;
    private ServerEndpoint serverEndpoint2;
    private ServerEndpoint serverEndpoint3;
    private AssetTree tree3;
    private AssetTree tree1;
    private AssetTree tree2;

    private String name;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws IOException {

        name = testName.getMethodName();

        Files.deleteIfExists(Paths.get(OS.TARGET, name.toString()));

        System.setProperty("EngineReplication.Compression", "gzip");

        YamlLogging.setAll(false);

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        // Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

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
    }


    public void preAfter() {
        if (serverEndpoint1 != null)
            serverEndpoint1.close();
        if (serverEndpoint2 != null)
            serverEndpoint2.close();
        if (serverEndpoint3 != null)
            serverEndpoint3.close();

        if (tree1 != null)
            tree1.close();
        if (tree2 != null)
            tree2.close();
        if (tree3 != null)
            tree3.close();
    }

    @NotNull
    private AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x))
                .withConfig(resourcesDir() + "/3wayLegacy", OS.TARGET + "/" + hostId);

        tree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).cluster(clusterTwo),
                        asset));

        //  VanillaAssetTreeEgMain.registerTextViewofTree("host " + hostId, tree);

        return tree;
    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }


    @Test
    public void testThreeWay() throws InterruptedException {
        YamlLogging.setAll(true);

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(name, String.class, String
                .class);
        assertNotNull(map1);

        map1.put("hello1", "world1");

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(name, String.class, String
                .class);
        assertNotNull(map2);

        map2.put("hello2", "world2");

        final ConcurrentMap<String, String> map3 = tree3.acquireMap(name, String.class, String
                .class);
        assertNotNull(map3);

        map3.put("hello3", "world3");

        for (int i = 1; i <= 100; i++) {
            if (map1.size() == 3 &&
                    map2.size() == 3 &&
                    map3.size() == 3)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2, map3}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals("world3", m.get("hello3"));
            Assert.assertEquals(3, m.size());
        }
    }
}

