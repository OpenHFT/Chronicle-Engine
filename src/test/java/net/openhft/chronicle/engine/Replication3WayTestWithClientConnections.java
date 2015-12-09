package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */

public class Replication3WayTestWithClientConnections {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String NAME = "/ChMaps/test";
    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;
    public static ServerEndpoint serverEndpoint3;
    private static AssetTree tree3;
    private static AssetTree tree1;
    private static AssetTree tree2;
    private static AssetTree clientEndpoint2;


    @BeforeClass
    public static void before() throws IOException {
        YamlLogging.clientWrites = true;
        YamlLogging.clientReads = true;

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2", "host.port3");

        WireType writeType = WireType.TEXT;
        tree1 = create(1, writeType, "clusterTwo");
        tree2 = create(2, writeType, "clusterTwo");
        tree3 = create(3, writeType, "clusterThree");

        serverEndpoint1 = new ServerEndpoint("host.port1", tree1, writeType);
        serverEndpoint2 = new ServerEndpoint("host.port2", tree2, writeType);
        serverEndpoint3 = new ServerEndpoint("host.port3", tree3, writeType);
        clientEndpoint2 = new VanillaAssetTree("client1").forRemoteAccess
                ("host.port2", WIRE_TYPE);
    }

    @AfterClass
    public static void after() throws IOException {
        if (serverEndpoint1 != null)
            serverEndpoint1.close();
        if (serverEndpoint2 != null)
            serverEndpoint2.close();
        if (serverEndpoint3 != null)
            serverEndpoint3.close();
        if (clientEndpoint2 != null)
            clientEndpoint2.close();

        if (tree1 != null)
            tree1.close();
        if (tree2 != null)
            tree2.close();
        if (tree3 != null)
            tree3.close();

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
    public void testThreeWayCheckClientSubscribeCount() throws InterruptedException {

        final char[] chars = new char[2 << 20];
        Arrays.fill(chars, 'X');
        final String largeString = new String(chars);

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map1);

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map2);


        AtomicInteger countUpdates = new AtomicInteger();
        clientEndpoint2.registerSubscriber(NAME, MapEvent.class, new Subscriber<MapEvent>() {
            @Override
            public void onMessage(MapEvent mapEvent) throws InvalidSubscriberException {
                countUpdates.incrementAndGet();
            }
        });

        Thread.sleep(500);

        map2.put("hello2", largeString);
        map2.put("hello1", largeString);
        map2.put("hello3", largeString);

        final ConcurrentMap<String, String> map3 = tree2.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map2);

        for (int i = 1; i <= 50; i++) {
            if (map1.size() == 3 &&
                    map2.size() == 3 &&
                    map3.size() == 3)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2, map3}) {
            Assert.assertEquals(largeString, m.get("hello1"));
            Assert.assertEquals(largeString, m.get("hello2"));
            Assert.assertEquals(largeString, m.get("hello3"));
            Assert.assertEquals(3, m.size());
        }

        Assert.assertEquals(3, countUpdates.get());
    }

}

