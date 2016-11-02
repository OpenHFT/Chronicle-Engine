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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
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
import org.junit.*;
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

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */

public class MaunualReplication2WayTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String SERVER1 = "192.168.1.66";

    static {
        //System.setProperty("ReplicationHandler3", "true");
    }

    public ServerEndpoint serverEndpoint1;
    public ServerEndpoint serverEndpoint2;
    @Rule
    public TestName testName = new TestName();
    public String name;
    private AssetTree tree1;

    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptions;

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
        exceptions = Jvm.recordExceptions();
        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run

        TCPRegistry.setAlias("host.port1", "host1", 8081);
        TCPRegistry.setAlias("host.port2", "host2", 8081);
        WireType writeType = WireType.TEXT;
        tree1 = create((isHost1() ? 1 : 2), writeType, "clusterTwo");

        serverEndpoint1 = new ServerEndpoint("*:8081", tree1);

    }

    public void after() throws IOException {
        if (serverEndpoint1 != null)
            serverEndpoint1.close();

        if (tree1 != null)
            tree1.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @NotNull
    private AssetTree create(final int hostId, WireType writeType, final String clusterNam) {
        VanillaAssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting();

        Asset testBootstrap = tree.root().acquireAsset("testManualTesting");
        testBootstrap.addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);

        testBootstrap.addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).cluster(clusterNam),
                        asset));

        tree.withConfig(resourcesDir() + "/2way", OS.TARGET + "/" + hostId);

        return tree;
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void beforeTest() throws IOException {
        YamlLogging.setAll(false);
        before();
        name = testName.getMethodName();
        Files.deleteIfExists(Paths.get(OS.TARGET, name.toString()));
    }

    private boolean isHost1() {
        /*final String hostname = InetAddress.getByName("host1").toString().split("/")[1];
        return InetAddress.getLocalHost().toString().contains(hostname);
        */
        return true;
    }

    @Ignore("manual test")
    @Test
    public void testManualTesting() throws InterruptedException, UnknownHostException {
        // YamlLogging.setAll(false);
        InetAddress hostname = InetAddress.getLocalHost();
        String hostName = hostname.toString();
        boolean isHost1 = isHost1();

        final ConcurrentMap<Integer, String> map1 = tree1.acquireMap(name, Integer.class, String.class);
        assertNotNull(map1);

        for (int i = 0; i < 99; i++) {
            int offset = isHost1 ? 0 : 1;
            map1.put((i * 2) + offset, hostName);

            Jvm.pause(5);

            Map<Integer, String> m = new TreeMap<>(map1);

            System.out.println("----------------------");

            m.entrySet().forEach(e -> System.out.println("" + e.getKey() + " \t" + e.getValue()));

            Jvm.pause(5000);
        }
    }
}

