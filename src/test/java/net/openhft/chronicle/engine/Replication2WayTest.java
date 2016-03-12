package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
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
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */

public class Replication2WayTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;

    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;

    private static AssetTree tree1;
    private static AssetTree tree2;
    private static AtomicReference<Throwable> t = new AtomicReference();
    @Rule
    public TestName testName = new TestName();
    public String name;

    @BeforeClass
    public static void before() throws IOException {
        YamlLogging.setAll(false);

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run

        TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");

        WireType writeType = WireType.TEXT;
        tree1 = create(1, writeType, "clusterTwo");
        tree2 = create(2, writeType, "clusterTwo");

        serverEndpoint1 = new ServerEndpoint("host.port1", tree1);
        serverEndpoint2 = new ServerEndpoint("host.port2", tree2);
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
    public void beforeTest() throws IOException {
        name = testName.getMethodName();
        Files.deleteIfExists(Paths.get(OS.TARGET, name.toString()));
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) Jvm.rethrow(th);
    }

    @Test
    public void testBootstrap() throws InterruptedException {

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(name, String.class, String
                .class);
        assertNotNull(map1);

        map1.put("hello1", "world1");

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(name, String.class, String
                .class);
        assertNotNull(map2);

        map2.put("hello2", "world2");

        for (int i = 1; i <= 50; i++) {
            if (map1.size() == 2 && map2.size() == 2)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals(2, m.size());
        }
    }

    @Test
    public void testBootstrapAllFromMap1() throws InterruptedException {

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(name, String.class, String
                .class);
        assertNotNull(map1);

        map1.put("hello1", "world1");
        map1.put("hello2", "world2");

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(name, String.class, String
                .class);
        assertNotNull(map2);

        for (int i = 1; i <= 50; i++) {
            if (map1.size() == 2 && map2.size() == 2)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals(2, m.size());
        }
    }

    @Test
    public void testBootstrapAllFromMap2() throws InterruptedException {

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(name, String.class, String
                .class);
        assertNotNull(map1);

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(name, String.class, String
                .class);

        map2.put("hello1", "world1");
        map2.put("hello2", "world2");

        assertNotNull(map2);

        for (int i = 1; i <= 50; i++) {
            if (map1.size() == 2 && map2.size() == 2)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals(2, m.size());
        }
    }

    @Test
    public void testBootstrapAllFromMap1WithSubscription() throws InterruptedException {

        AtomicInteger map1Updates = new AtomicInteger();
        AtomicInteger map2Updates = new AtomicInteger();

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(name, String.class, String
                .class);
        assertNotNull(map1);
        tree1.registerSubscriber(name, MapEvent.class, f -> {
            map1Updates.incrementAndGet();
        });
        Thread.sleep(1);
        map1.put("hello1", "world1");
        Thread.sleep(1);
        map1.put("hello2", "world2");
        Thread.sleep(1);

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(name, String.class, String
                .class);
        tree1.registerSubscriber(name, MapEvent.class, f -> {
            map2Updates.incrementAndGet();
        });

        assertNotNull(map2);

        for (int i = 1; i <= 50; i++) {
            if (map1.size() == 2 && map2.size() == 2)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals(2, m.size());
        }

        Assert.assertEquals(2, map1Updates.get());
        Assert.assertEquals(2, map2Updates.get());
    }


    @Test
    public void testBootstrapAllFromMap1WithSubscription2() throws InterruptedException {

        AtomicInteger map1Updates = new AtomicInteger();

        AtomicInteger map2Updates = new AtomicInteger();

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(name, String.class, String
                .class);
        assertNotNull(map1);
        tree1.registerSubscriber(name, MapEvent.class, f -> map1Updates.incrementAndGet());
        map1.clear();
        map1.put("hello1", "world1");

        map1.put("hello2", "world2");
        Thread.sleep(2);

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(name, String.class, String
                .class);

        Thread.sleep(1000);

        tree2.registerSubscriber(name, MapEvent.class, f -> map2Updates.incrementAndGet());

        map2.put("hello1", "world1");
        map2.put("hello2", "world2");

        assertNotNull(map2);

        for (int i = 1; i <= 50; i++) {
            if (map1.size() == 2 && map2.size() == 2)
                break;
            Jvm.pause(300);
        }

        for (Map m : new Map[]{map1, map2}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals(2, m.size());
        }

        Assert.assertEquals(2, map1Updates.get());
        Assert.assertEquals(2, map2Updates.get());
    }

}

