package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Created by daniel on 28/05/15.
 */
public class ChronicleMapKeyValueStoreTest {
    public static final String NAME = "chronmapkvstoretests3";

    private static AssetTree tree1;
    private static AssetTree tree2;
    private static AssetTree tree3;

    @BeforeClass
    public static void before() throws IOException {
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));
        tree1 = create("1");
        tree2 = create("2");
        tree3 = create("3");
    }

    @AfterClass
    public static void after() {
        tree1.close();
        tree2.close();
        tree3.close();
    }

    private static AssetTree create(final String node) {
        Function<Bytes, Wire> writeType = TextWire::new;
        AssetTree tree1 = new VanillaAssetTree(1).forTesting();

        tree1.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree1.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree1.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType),
                        asset));

        return tree1;
    }

    @Ignore("todo fix")
    @Test
    public void test() throws Exception {



        final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map1);

        final ConcurrentMap<String, String> map2 = tree1.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map2);

        final ConcurrentMap<String, String> map3 = tree1.acquireMap(NAME, String.class, String
                .class);
        assertNotNull(map3);

        final EngineReplication replicator1 = tree1.acquireService(NAME, EngineReplication.class);
        assertNotNull(replicator1);

        final EngineReplication replicator2 = tree2.acquireService(NAME, EngineReplication.class);
        assertNotNull(replicator2);

        final EngineReplication replicator3 = tree3.acquireService(NAME, EngineReplication.class);
        assertNotNull(replicator3);


        final ModificationIterator iterator1for2 = replicator1.acquireModificationIterator
                (replicator2.identifier());

        final ModificationIterator iterator1for3 = replicator1.acquireModificationIterator
                (replicator3.identifier());

        final ModificationIterator iterator2for1 = replicator2.acquireModificationIterator
                (replicator1.identifier());

        final ModificationIterator iterator2for3 = replicator2.acquireModificationIterator
                (replicator3.identifier());

        final ModificationIterator iterator3for1 = replicator3.acquireModificationIterator
                (replicator1.identifier());

        final ModificationIterator iterator3for2 = replicator3.acquireModificationIterator
                (replicator2.identifier());

        map1.put("hello1", "world1");
        map2.put("hello2", "world2");
        map3.put("hello3", "world3");

        iterator1for2.forEach(replicator2.identifier(), replicator2::applyReplication);
        iterator1for3.forEach(replicator3.identifier(), replicator3::applyReplication);

        iterator2for1.forEach(replicator1.identifier(), replicator1::applyReplication);
        iterator2for3.forEach(replicator3.identifier(), replicator3::applyReplication);

        iterator3for1.forEach(replicator1.identifier(), replicator1::applyReplication);
        iterator3for2.forEach(replicator2.identifier(), replicator2::applyReplication);

        for (Map m : new Map[]{map1, map2, map3}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals("world3", m.get("hello3"));
            Assert.assertEquals(3, m.size());
        }



    }

}
