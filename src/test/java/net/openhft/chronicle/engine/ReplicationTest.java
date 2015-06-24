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
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */
@Ignore
public class ReplicationTest {

    public static final String NAME = "/ChMaps/test";

    private static AssetTree tree1;
    private static AssetTree tree2;
//    private static AssetTree tree3;

    public static ServerEndpoint serverEndpoint1;
    public static ServerEndpoint serverEndpoint2;
//    public static ServerEndpoint serverEndpoint3;

    @BeforeClass
    public static void before() throws IOException {
        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        tree1 = create(1);
        tree2 = create(2);
//        tree3 = create(3);

        serverEndpoint1 = new ServerEndpoint(8080, true, tree1);
        serverEndpoint2 = new ServerEndpoint(8081, true, tree2);
//        serverEndpoint3 = new ServerEndpoint(8082, true, tree3);

    }

    @AfterClass
    public static void after() {
        serverEndpoint1.close();
        serverEndpoint2.close();
//        serverEndpoint3.close();
        tree1.close();
        tree2.close();
//        tree3.close();
    }

    private static AssetTree create(final int hostId) {
        Function<Bytes, Wire> writeType = WireType.TEXT;
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting()
                .withConfig(resourcesDir() + "/cmkvst", OS.TARGET + "/" + hostId);

        tree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType),
                        asset));

        VanillaAssetTreeEgMain.registerTextViewofTree("host " + hostId, tree);

        // tree.root().addView(HostIdentifier.class);

        System.out.println(tree.toString());
        //System.out.println(host);

        return tree;
    }

    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    @Test
    public void test() throws Exception {

        final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map1);

        final ConcurrentMap<String, String> map2 = tree2.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map2);

/*
        final ConcurrentMap<String, String> map3 = tree3.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map3);
*/

        map1.put("hello1", "world1");
        map2.put("hello2", "world2");
//        map3.put("hello3", "world3");

        Thread.sleep(1000);

        for (Map m : new Map[]{map1, map2/*, map3*/}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
//            Assert.assertEquals("world3", m.get("hello3"));
            Assert.assertEquals(2, m.size());
        }
    }

}

