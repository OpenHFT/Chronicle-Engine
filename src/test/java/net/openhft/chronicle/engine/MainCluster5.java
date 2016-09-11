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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rob Austin
 */

public class MainCluster5 {
    public static final WireType WIRE_TYPE = WireType.BINARY;
    public static final int entries = 50;
    public static final String basePath = OS.TARGET + '/' + System.getProperty("server", "one");
    public static final String CLUSTER = System.getProperty("cluster", "clusterFive");
    static final int VALUE_SIZE = 1 << 20;
    public static final String NAME1 = "/ChMaps/test1" +
            "?entries=" + entries +
            "&putReturnsNull=true" +
            "&averageValueSize=" + VALUE_SIZE;
    public static final String NAME2 = "/ChMaps/test2" +
            "?entries=" + entries +
            "&putReturnsNull=true" +
            "&averageValueSize=" + VALUE_SIZE;
    //+    //"&basePath=/" + basePath;
    public static ServerEndpoint serverEndpoint;

    private static AssetTree tree;
    private static Map<ExceptionKey, Integer> exceptions;

    public static void before() throws IOException {
        exceptions = Jvm.recordExceptions();
        System.out.println("Using cluster " + CLUSTER + " basePath: " + basePath);
        YamlLogging.setAll(false);
        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(basePath, "test"));

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;

        switch (System.getProperty("server", "one")) {
            case "one":

                tree = create(1, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint(":8081", tree);
                break;

            case "two":
                tree = create(2, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint(":8082", tree);
                break;

            case "three":
                tree = create(3, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint(":8083", tree);
                break;

            case "four":
                tree = create(4, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint(":8084", tree);
                break;

            case "five":
                tree = create(5, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint(":8085", tree);
                break;

            case "client":
                tree = new VanillaAssetTree("/").forRemoteAccess
                        ("localhost:9093", WIRE_TYPE);
        }
        // configure them
        tree.acquireMap(NAME1, String.class, String.class).size();
        tree.acquireMap(NAME2, String.class, String.class).size();
    }

    @AfterClass
    public static void after() {

        if (serverEndpoint != null)
            serverEndpoint.close();

        if (tree != null)
            tree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        // TODO TCPRegistery.assertAllServersStopped();
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @NotNull
    static AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
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

    @NotNull
    public static String getKey(int i) {
        return "" + i;
    }

    public static String generateValue(char c) {
        char[] chars = new char[100];
        Arrays.fill(chars, c);

        // with snappy this results in about 10:1 compression.
        //Random rand = new Random();
        // for (int i = 0; i < chars.length; i += 45)
        //     chars[rand.nextInt(chars.length)] = '.';
        return new String(chars);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        before();
        new MainCluster5().test();
        after();
    }

    public void test() throws InterruptedException, IOException {

        //  YamlLogging.setAll(false);

        final String type = System.getProperty("server", "one");

        final ConcurrentMap<String, String> map1 = tree.acquireMap(NAME1, String.class, String.class);
        final ConcurrentMap<String, String> map2 = tree.acquireMap(NAME2, String.class, String.class);
        if ("one".equals(type)) {
            for (int i = 1; i < entries; i += 10) {
                map1.put(getKey(i), generateValue('1'));
            }
        }
        if ("two".equals(type)) {
            for (int i = 2; i < entries; i += 10) {
                map2.put(getKey(i), generateValue('2'));
            }
        }

        for (; ; ) {
            System.out.println("\n[ Map Contents " + LocalDateTime.now() + " )]");
            map1.forEach((k, v) -> System.out.print("1: k=" + k + ", v=" + (v == null ? "null" : v
                    .substring(0, v.length() < 20 ? v.length() : 20)) + "\t"));
            System.out.println(".");
            map2.forEach((k, v) -> System.out.print("1: k=" + k + ", v=" + (v == null ? "null" : v
                    .substring(0, v.length() < 20 ? v.length() : 20)) + "\t"));
            System.out.println(".");
            Jvm.pause(10_000);
        }
    }
}

