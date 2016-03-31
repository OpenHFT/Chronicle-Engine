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
import net.openhft.chronicle.engine.api.tree.Asset;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */

public class RobsReplication2WayTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String SERVER1 = "192.168.1.66";

    static {
        System.setProperty("ReplicationHandler3", "true");
    }

    public ServerEndpoint serverEndpoint1;
    public ServerEndpoint serverEndpoint2;
    @Rule
    public TestName testName = new TestName();
    public String name;
    private AssetTree tree1;
    private AtomicReference<Throwable> t = new AtomicReference();

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    public static void main(String[] args) throws UnknownHostException {
        System.out.println(InetAddress.getLocalHost());
    }

    public void before() throws IOException {


        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run

        TCPRegistry.createServerSocketChannelFor(
                "clusterThree.host.port1",
                "clusterThree.host.port2",
                "clusterThree.host.port3",
                "host.port1",
                "host.port2");

        WireType writeType = WireType.TEXT;
        tree1 = create((isHost1() ? 1 : 2), writeType, "clusterRob");

        serverEndpoint1 = new ServerEndpoint("*:8081", tree1);
        //   serverEndpoint2 = new ServerEndpoint("host.port2", tree2);
    }

    public void after() throws IOException {
        if (serverEndpoint1 != null)
            serverEndpoint1.close();

        if (tree1 != null)
            tree1.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        // TODO TCPRegistery.assertAllServersStopped();
    }

    @NotNull
    private AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
        VanillaAssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x));

        Asset testBootstrap = tree.root().acquireAsset("testBootstrap");
        testBootstrap.addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);

        testBootstrap.addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        testBootstrap.addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).cluster(clusterTwo),
                        asset));

        tree.withConfig(resourcesDir() + "/cmkvst", OS.TARGET + "/" + hostId);

        return tree;
    }

    @Before
    public void beforeTest() throws IOException {
        YamlLogging.setAll(false);
        before();
        name = testName.getMethodName();
        Files.deleteIfExists(Paths.get(OS.TARGET, name.toString()));
    }

    @After
    public void afterMethod() throws IOException {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
        after();
    }

    private boolean isHost1() throws UnknownHostException {
        /*final String hostname = InetAddress.getByName("host1").toString().split("/")[1];
        return InetAddress.getLocalHost().toString().contains(hostname);
        */
        return true;
    }

    // @Ignore
    @Test
    public void testBootstrap() throws InterruptedException, UnknownHostException {
        YamlLogging.showServerReads(true);

        InetAddress hostname = InetAddress.getLocalHost();
        String hostName = hostname.toString();
        boolean isHost1 = isHost1();

        final ConcurrentMap<Integer, String> map1 = tree1.acquireMap(name, Integer.class, String.class);
        assertNotNull(map1);


        for (int i = 0; i < 99; i++) {
            int offset = isHost1 ? 0 : 1;
            map1.put((i * 2) + offset, hostName);

            Thread.sleep(5);

            Map<Integer, String> m = new TreeMap<>(map1);

            System.out.println("----------------------");

            m.entrySet().forEach(e -> System.out.println("" + e.getKey() + " \t" + e.getValue()));

            Thread.sleep(5000);
        }


    }
}

