package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Created by Rob Austin
 */

public class TestReplicationCluster {
    public static final WireType WIRE_TYPE = WireType.BINARY;
    public static final int entries = 300;
    public static final int SIZE = 1024;
    public static final String NAME = "/ChMaps/test?entries=" + entries + "&averageValueSize=" +
            (2 << 20);

    public static ServerEndpoint serverEndpoint;

    private static AssetTree tree;

    private static ServerEndpoint serverEndpoint1;
    private static ServerEndpoint serverEndpoint2;
    private static ServerEndpoint serverEndpoint3;
    private static ServerEndpoint serverEndpoint4;
    private static ServerEndpoint serverEndpoint5;

    @BeforeClass
    public static void before() throws IOException {
        YamlLogging.clientWrites = false;
        YamlLogging.clientReads = false;

        //YamlLogging.showServerWrites = true;
        TCPRegistry.createServerSocketChannelFor(
                "host.port1",
                "host.port2",
                "host.port3",
                "host.port4",
                "host.port5");
        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;
        {
            AssetTree tree = create(1, writeType, "clusterFive");
            serverEndpoint1 = new ServerEndpoint("host.port1", tree, writeType);
            tree.acquireMap(NAME, String.class,
                    String.class).size();
        }

        {
            AssetTree tree = create(2, writeType, "clusterFive");
            serverEndpoint2 = new ServerEndpoint("host.port2", tree, writeType);
            tree.acquireMap(NAME, String.class,
                    String.class).size();
        }

        {
            AssetTree tree = create(3, writeType, "clusterFive");
            serverEndpoint3 = new ServerEndpoint("host.port3", tree, writeType);
            tree.acquireMap(NAME, String.class,
                    String.class).size();
        }


        {
            AssetTree tree = create(4, writeType, "clusterFive");
            serverEndpoint4 = new ServerEndpoint("host.port4", tree, writeType);
            tree.acquireMap(NAME, String.class,
                    String.class).size();
        }


        {
            AssetTree tree = create(5, writeType, "clusterFive");
            serverEndpoint5 = new ServerEndpoint("host.port5", tree, writeType);
            tree.acquireMap(NAME, String.class,
                    String.class).size();
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
    private static AssetTree create(final int hostId, Function<Bytes, Wire> writeType, final String clusterTwo) {
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

    public static void main(String[] args) throws IOException, InterruptedException {
        before();
        new TestReplicationCluster().test();
        after();
    }

    @NotNull
    public static String getKey(int i) {
        return "key" + i;
    }

    public static String generateValue(char c) {
        char[] chars = new char[SIZE / 2];
        Arrays.fill(chars, c);

        // with snappy this results in about 10:1 compression.
        //Random rand = new Random();
        // for (int i = 0; i < chars.length; i += 45)
        //     chars[rand.nextInt(chars.length)] = '.';
        return new String(chars);
    }


    @Ignore
    @Test
    public void test() throws InterruptedException, IOException {

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        AtomicInteger count = new AtomicInteger();

        final String s = generateValue('X');
        Executors.newSingleThreadExecutor().submit(() -> {
            VanillaAssetTree tree1 = new VanillaAssetTree("/").forRemoteAccess("host.port1",
                    WIRE_TYPE);
            final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class,
                    String.class);
            for (; count.get() < 500; ) {
                for (int i = 0; i < 50; i++) {
                    map1.put("" + i, s);
                    try {
                        Thread.sleep(200);
                        count.incrementAndGet();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }


        });

        Thread.sleep(500);
        YamlLogging.setAll(false);


        final ConcurrentMap<String, String> map;
        AssetTree tree3 = new VanillaAssetTree("/").forRemoteAccess("host.port3", WIRE_TYPE);


        tree3.registerSubscriber(NAME, MapEvent.class, o ->

        {
            System.out.println((o == null) ? "null" : (o.toString()
                    .length() > 150 ? o.toString().substring(0, 150) : "XXXX"));
            count.decrementAndGet();
        });


        for (; count.get() > 0; ) {
            try {
                Thread.sleep(5000);
            } catch (Exception ignore) {

            }
        }

    }


}

