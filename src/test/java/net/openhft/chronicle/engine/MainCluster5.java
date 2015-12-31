package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
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
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Created by Rob Austin
 */

public class MainCluster5 {
    public static final WireType WIRE_TYPE = WireType.BINARY;
    public static final int entries = 150;
    public static final String basePath = OS.TARGET + '/' + System.getProperty("server", "one");
    public static final String NAME = "/ChMaps/test?entries=" + entries +
            "&averageValueSize=" + MainClusterClient.VALUE_SIZE;
    public static final String CLUSTER = System.getProperty("cluster", "clusterFive");
    //+    //"&basePath=/" + basePath;
    public static ServerEndpoint serverEndpoint;

    private static AssetTree tree;


    public static void before() throws IOException {
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
                serverEndpoint = new ServerEndpoint("localhost:8081", tree, writeType);
                tree.acquireMap(NAME, String.class, String.class).size();
                break;

            case "two":
                tree = create(2, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint("localhost:8082", tree, writeType);
                tree.acquireMap(NAME, String.class, String.class).size();
                break;


            case "three":
                tree = create(3, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint("localhost:8083", tree, writeType);
                tree.acquireMap(NAME, String.class, String.class).size();
                break;


            case "four":
                tree = create(4, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint("localhost:8084", tree, writeType);
                tree.acquireMap(NAME, String.class, String.class).size();
                break;


            case "five":
                tree = create(5, writeType, CLUSTER);
                serverEndpoint = new ServerEndpoint("localhost:8085", tree, writeType);
                tree.acquireMap(NAME, String.class, String.class).size();
                break;

            case "client":
                tree = new VanillaAssetTree("/").forRemoteAccess
                        ("localhost:9093", WIRE_TYPE, t -> t.printStackTrace());
                tree.acquireMap(NAME, String.class, String.class).size();

        }


    }

    @AfterClass
    public static void after() throws IOException {


        if (serverEndpoint != null)
            serverEndpoint.close();

        if (tree != null)
            tree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        // TODO TCPRegistery.assertAllServersStopped();
    }

    @NotNull
    static AssetTree create(final int hostId, Function<Bytes, Wire> writeType, final String clusterTwo) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(t -> t.printStackTrace())
                .withConfig(resourcesDir() + "/cmkvst", OS.TARGET + "/" + hostId);

        tree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).cluster(clusterTwo),
                        asset));

        VanillaAssetTreeEgMain.registerTextViewofTree("host " + hostId, tree);

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

        //  YamlLogging.setAll(true);


        final ConcurrentMap<String, String> map;
        final String type = System.getProperty("server", "one");

        map = tree.acquireMap(NAME, String.class, String.class);
        if ("one".equals(type)) {
            for (int i = 0; i < entries; i++) {
//                map.put(getKey(i), generateValue('1'));
            }

            //   System.in.read();
        }


        if ("five".equals(type)) {
            for (int i = 0; i < entries; i++) {
                //         map.put(getKey(i), generateValue('5'));
            }

            //   System.in.read();
        }


        for (; ; ) {
            map.forEach((k, v) -> System.out.println("k=" + k + ", v=" + (v == null ? "null" : v
                    .substring(1, v.length() < 50 ? v.length() : 50))));
            System.out.println("\n\n");
            Thread.sleep(5000);
        }

    }
}

