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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rob Austin
 */

public class Main2Way {
    public static final WireType WIRE_TYPE = WireType.COMPRESSED_BINARY;
    public static final int entries = 1000;
    public static final String NAME = "/ChMaps/test?entries=" + entries + "&averageValueSize=" + (2 << 20);
    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;

    private static AssetTree tree3;
    private static AssetTree tree1;
    private static AssetTree tree2;
    private static Map<ExceptionKey, Integer> exceptions;

    @BeforeClass
    public static void before() throws IOException {
        exceptions = Jvm.recordExceptions();
        YamlLogging.setAll(false);

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;

        if ("one".equals(System.getProperty("server", "one"))) {
            tree1 = create(1, writeType, "clusterThree");
            serverEndpoint1 = new ServerEndpoint("localhost:8081", tree1);
        } else {
            tree2 = create(2, writeType, "clusterThree");
            serverEndpoint2 = new ServerEndpoint("localhost:8082", tree2);
        }
    }

    @AfterClass
    public static void after() {

        if (serverEndpoint1 != null)
            serverEndpoint1.close();
        if (serverEndpoint2 != null)
            serverEndpoint2.close();

        if (tree1 != null)
            tree1.close();
        if (tree2 != null)
            tree2.close();

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
                .withConfig(resourcesDir() + "/cmkvst", OS.TARGET + "/" + hostId);

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

    public static void main(String[] args) throws IOException, InterruptedException {
        before();
        new Main2Way().test();
        after();
    }

    @NotNull
    public static String getKey(int i) {
        return "key" + i;
    }

    public static String generateValue() {
        char[] chars = new char[2 << 20];
        Arrays.fill(chars, 'X');

        // with snappy this results in about 10:1 compression.
        Random rand = new Random();
        for (int i = 0; i < chars.length; i += 45)
            chars[rand.nextInt(chars.length)] = '.';
        return new String(chars);
    }

    public void test() throws InterruptedException, IOException {

        YamlLogging.setAll(false);

        String data = generateValue();

        final ConcurrentMap<String, String> map;
        final String type = System.getProperty("server", "one");
        if ("one".equals(type)) {
            map = tree1.acquireMap(NAME, String.class, String.class);
        } else {
            map = tree2.acquireMap(NAME, String.class, String.class);
        }

        for (int i = 0; i < entries; i++) {
            map.put(getKey(i), data);
        }

        System.in.read();
    }
}

