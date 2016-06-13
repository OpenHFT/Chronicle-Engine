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
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
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
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

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
@Ignore
public class Replication10WayTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String NAME = "/ChMaps/test";
    public static final int NUMBER_OF_SIMULATED_SERVERS = 10;
    public static ServerEndpoint[] serverEndpoint = new
            ServerEndpoint[NUMBER_OF_SIMULATED_SERVERS];
    private static AssetTree[] tree = new AssetTree[NUMBER_OF_SIMULATED_SERVERS];
    private static Map<ExceptionKey, Integer> exceptions;
    private ThreadDump threadDump;

    @BeforeClass
    public static void before() throws IOException {
        exceptions = Jvm.recordExceptions();
        YamlLogging.setAll(false);

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        for (int i = 0; i < NUMBER_OF_SIMULATED_SERVERS; i++) {
            TCPRegistry.createServerSocketChannelFor("host.port" + (i + 1));
        }

        WireType writeType = WireType.TEXT;
        for (int i = 0; i < NUMBER_OF_SIMULATED_SERVERS; i++) {
            tree[i] = create(i + 1, writeType, "clusterTen");
            serverEndpoint[i] = new ServerEndpoint("host.port" + (i + 1), tree[i]);
        }

    }

    @AfterClass
    public static void after() {

        for (int i = 0; i < NUMBER_OF_SIMULATED_SERVERS; i++) {
            serverEndpoint[i].close();
        }

        for (int i = 0; i < NUMBER_OF_SIMULATED_SERVERS; i++) {
            tree[i].close();
        }

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @NotNull
    private static AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting()
                .withConfig(resourcesDir() + "/10Way", OS.TARGET + "/" + hostId);

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

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Ignore
    @Test
    public void testTenWay() throws InterruptedException {

        ConcurrentMap<String, String>[] maps = new ConcurrentMap[NUMBER_OF_SIMULATED_SERVERS];
        for (int i = 0; i < NUMBER_OF_SIMULATED_SERVERS; i++) {
            maps[i] = tree[i].acquireMap(NAME, String.class, String.class);
            assertNotNull(maps[i]);
            maps[i].put("hello" + (i + 1), "world" + (i + 1));
        }

        OUTER:
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < NUMBER_OF_SIMULATED_SERVERS; j++) {
                final int size = maps[j].size();
                if (size != NUMBER_OF_SIMULATED_SERVERS) {
                    Jvm.pause(300);
                    continue OUTER;
                }
            }
            System.out.println("got all ten");
        }

        for (int i = 0; i < NUMBER_OF_SIMULATED_SERVERS; i++) {
            for (int j = 0; j < NUMBER_OF_SIMULATED_SERVERS; j++) {
                Assert.assertEquals("world" + (j + 1), maps[i].get("hello" + (j + 1)));
            }
        }
    }
}

