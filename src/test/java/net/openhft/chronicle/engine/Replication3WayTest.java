package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.END_SUBSCRIPTION_AFTER_BOOTSTRAP;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */

@RunWith(value = Parameterized.class)

public class Replication3WayTest {


    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String NAME = "/ChMaps/test";
    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;
    public static ServerEndpoint serverEndpoint3;
    static RequestContext.Operation operation = END_SUBSCRIPTION_AFTER_BOOTSTRAP;
    private static AssetTree tree3;
    private static AssetTree tree1;
    private static AssetTree tree2;
    private static AtomicReference<Throwable> t = new AtomicReference();

    static {
        System.setProperty("ReplicationHandler3", "true");
    }

    private final WireType wireType;

    public Replication3WayTest(WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        final int initialCapacity = 3;
        final List<Object[]> list = new ArrayList<>(initialCapacity);

        for (int i = 0; i < initialCapacity; i++) {
            list.add(new Object[]{WireType.BINARY});
        }

        for (int i = 0; i < initialCapacity; i++) {
            list.add(new Object[]{WireType.TEXT});
        }

        return list;
    }

    @NotNull
    private static AssetTree create(final int hostId, WireType writeType, final String clusterTwo) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x))
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

    @Before
    public void before() throws IOException, InterruptedException {
        YamlLogging.setAll(false);

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        TCPRegistry.createServerSocketChannelFor(
                "clusterThree.host.port1",
                "clusterThree.host.port2",
                "clusterThree.host.port3",
                "host.port1",
                "host.port2",
                "host.port3");


        tree1 = create(1, wireType, "clusterThree");
        tree2 = create(2, wireType, "clusterThree");
        tree3 = create(3, wireType, "clusterThree");

        serverEndpoint1 = new ServerEndpoint("clusterThree.host.port1", tree1);
        serverEndpoint2 = new ServerEndpoint("clusterThree.host.port2", tree2);
        serverEndpoint3 = new ServerEndpoint("clusterThree.host.port3", tree3);


    }

    @After
    public void after() throws IOException, InterruptedException {
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

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();

        Thread.sleep(500);
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Test
    public void testThreeWay() throws InterruptedException {
        //   YamlLogging.setAll(true);
        //YamlLogging.showServerReads = true;

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map1);

        map1.put("hello1", "world1");

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map2);

        map2.put("hello2", "world2");

        final ConcurrentMap<String, String> map3 = tree3.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map2);

        map3.put("hello3", "world3");

        for (int i = 1; i <= 100; i++) {
            if (map1.size() == 3 &&
                    map2.size() == 3 &&
                    map3.size() == 3)
                break;
            if (i == 99)
                System.out.println("");
            Jvm.pause(100);
        }


        for (Map m : new Map[]{map1, map2, map3}) {

            if (m.size() != 3)
                System.out.println("");
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals("world3", m.get("hello3"));
            Assert.assertEquals(3, m.size());
        }
    }
}

