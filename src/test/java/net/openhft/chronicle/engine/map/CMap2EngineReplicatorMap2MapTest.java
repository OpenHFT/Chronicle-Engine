package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;

/**
 * Created by Rob Austin
 */

public class CMap2EngineReplicatorMap2MapTest {

    final CMap2EngineReplicator replicator1 = new CMap2EngineReplicator(null);
    final ChronicleMap<String, String> map1 = newMap(1, replicator1, String.class, String.class);

    final CMap2EngineReplicator replicator2 = new CMap2EngineReplicator(null);
    final ChronicleMap<String, String> map2 = newMap(2, replicator2, String.class, String.class);

    final CMap2EngineReplicator replicator3 = new CMap2EngineReplicator(null);
    final ChronicleMap<String, String> map3 = newMap(3, replicator3, String.class, String.class);


    public <K, V> ChronicleMap<K, V> newMap(int localIdentifier,
                                            final CMap2EngineReplicator replicator,
                                            @NotNull final Class<K> keyClass,
                                            @NotNull final Class<V> valueClass) {
        return ChronicleMapBuilder.of(keyClass, valueClass).
                replication(builder().engineReplication(replicator).createWithId((byte) localIdentifier))
                .create();
    }

    /**
     * tests that the updates from one map are replicated to the other and visa versa
     */
    @Test
    public void testLocalPut() throws Exception {

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


    /**
     * tests that the updates from one map are replicated to the other and visa versa
     */
    @Test
    public void testLocalPutBootstrap() throws Exception {

        map1.put("hello1", "world1");
        map2.put("hello2", "world2");
        map3.put("hello3", "world3");

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

        iterator1for2.dirtyEntries(0);
        iterator1for2.forEach(replicator2.identifier(), replicator2::applyReplication);

        iterator1for3.dirtyEntries(0);
        iterator1for3.forEach(replicator3.identifier(), replicator3::applyReplication);

        iterator2for1.dirtyEntries(0);
        iterator2for1.forEach(replicator1.identifier(), replicator1::applyReplication);

        iterator2for3.dirtyEntries(0);
        iterator2for3.forEach(replicator3.identifier(), replicator3::applyReplication);

        iterator3for1.dirtyEntries(0);
        iterator3for1.forEach(replicator1.identifier(), replicator1::applyReplication);

        iterator3for2.dirtyEntries(0);
        iterator3for2.forEach(replicator2.identifier(), replicator2::applyReplication);

        for (Map m : new Map[]{map1, map2, map3}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals("world3", m.get("hello3"));
            Assert.assertEquals(3, m.size());
        }

    }


}

