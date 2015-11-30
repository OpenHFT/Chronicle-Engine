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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

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

public class Main2Way {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String NAME = "/ChMaps/test?entries=1000&averageValueSize=" + (2 << 20);
    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;

    private static AssetTree tree3;
    private static AssetTree tree1;
    private static AssetTree tree2;

    @BeforeClass
    public static void before() throws IOException {
        YamlLogging.clientWrites = true;
        YamlLogging.clientReads = true;

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.TEXT;

        if ("one".equals(System.getProperty("server", "one"))) {
            tree1 = create(1, writeType, "clusterThree");
            serverEndpoint1 = new ServerEndpoint("localhost:8081", tree1, writeType);
        } else {
            tree2 = create(2, writeType, "clusterThree");
            serverEndpoint2 = new ServerEndpoint("localhost:8082", tree2, writeType);
        }
    }

    @AfterClass
    public static void after() throws IOException {


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


    @Test
    @Ignore
    public void test() throws InterruptedException {

        YamlLogging.setAll(false);
        char[] chars = new char[(1 << 20) * 2];
        Arrays.fill(chars, 'X');
        String data = new String(chars);

        final ConcurrentMap<String, String> map;
        final String type = System.getProperty("server", "one");
        if ("one".equals(type)) {
            map = tree1.acquireMap(NAME, String.class, String.class);
        } else {
            map = tree2.acquireMap(NAME, String.class, String.class);
        }


        //    entries(50).averageValueSize(2 << 20)

        for (int i = 0; i < 500; i++) {
            map.put("key" + i, data);
        }


        for (; ; ) {
            Thread.sleep(5000);
        }

    }
}

