package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;

/**
 * Created by Rob Austin
 */

public class EngineReplicatorMap2MapTest {

    final EngineReplicator replicator1 = new EngineReplicator(null);
    final ChronicleMap<String, String> map1 = newMap(1, replicator1, String.class, String.class);

    final EngineReplicator replicator2 = new EngineReplicator(null);
    final ChronicleMap<String, String> map2 = newMap(2, replicator2, String.class, String.class);

    public <K, V> ChronicleMap<K, V> newMap(int localIdentifier,
                                            final EngineReplicator replicator,
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

        final ModificationIterator iterator1 = replicator1.acquireModificationIterator
                (replicator1.identifier());

        final ModificationIterator iterator2 = replicator2.acquireModificationIterator
                (replicator2.identifier());

        map1.put("hello1", "world1");
        map2.put("hello2", "world2");

        iterator1.forEach(replicator2.identifier(), replicator2::onEntry);
        iterator2.forEach(replicator1.identifier(), replicator1::onEntry);

        {
            Assert.assertEquals("world1", map1.get("hello1"));
            Assert.assertEquals("world2", map1.get("hello2"));
            Assert.assertEquals(2, map1.size());
        }

        {
            Assert.assertEquals("world1", map2.get("hello1"));
            Assert.assertEquals("world2", map2.get("hello2"));
            Assert.assertEquals(2, map2.size());
        }

    }

}

