/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;

/**
 * Created by Rob Austin
 */

public class CMap2EngineReplicatorMap2MapTest {

    @Nullable
    private final CMap2EngineReplicator replicator1 = new CMap2EngineReplicator(null);
    private final ChronicleMap<String, String> map1 = newMap(1, replicator1, String.class, String.class);

    @Nullable
    private final CMap2EngineReplicator replicator2 = new CMap2EngineReplicator(null);
    private final ChronicleMap<String, String> map2 = newMap(2, replicator2, String.class, String.class);

    @Nullable
    private final CMap2EngineReplicator replicator3 = new CMap2EngineReplicator(null);
    private final ChronicleMap<String, String> map3 = newMap(3, replicator3, String.class, String.class);
    private ThreadDump threadDump;

    private <K, V> ChronicleMap<K, V> newMap(int localIdentifier,
                                             final CMap2EngineReplicator replicator,
                                             @org.jetbrains.annotations.NotNull @NotNull final Class<K> keyClass,
                                             @org.jetbrains.annotations.NotNull @NotNull final Class<V> valueClass) {
        return ChronicleMapBuilder.of(keyClass, valueClass).
                replication(builder().engineReplication(replicator).createWithId((byte) localIdentifier))
                .create();
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    /**
     * tests that the updates from one map are replicated to the other and visa versa
     */
    @Test
    public void testLocalPut() {

        @Nullable final ModificationIterator iterator1for2 = replicator1.acquireModificationIterator
                (replicator2.identifier());

        @Nullable final ModificationIterator iterator1for3 = replicator1.acquireModificationIterator
                (replicator3.identifier());

        @Nullable final ModificationIterator iterator2for1 = replicator2.acquireModificationIterator
                (replicator1.identifier());

        @Nullable final ModificationIterator iterator2for3 = replicator2.acquireModificationIterator
                (replicator3.identifier());

        @Nullable final ModificationIterator iterator3for1 = replicator3.acquireModificationIterator
                (replicator1.identifier());

        @Nullable final ModificationIterator iterator3for2 = replicator3.acquireModificationIterator
                (replicator2.identifier());

        map1.put("hello1", "world1");
        map2.put("hello2", "world2");
        map3.put("hello3", "world3");

        while (iterator1for2.hasNext()) {
            iterator1for2.nextEntry(replicator2::applyReplication);
        }
        while (iterator1for3.hasNext()) {
            iterator1for3.nextEntry(replicator3::applyReplication);
        }

        while (iterator2for1.hasNext()) {
            iterator2for1.nextEntry(replicator1::applyReplication);
        }
        while (iterator2for3.hasNext()) {
            iterator2for3.nextEntry(replicator3::applyReplication);
        }

        while (iterator3for1.hasNext()) {
            iterator3for1.nextEntry(replicator1::applyReplication);
        }
        while (iterator3for2.hasNext()) {
            iterator3for2.nextEntry(replicator2::applyReplication);
        }

        for (@org.jetbrains.annotations.NotNull Map m : new Map[]{map1, map2, map3}) {
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
    public void testLocalPutBootstrap() throws InterruptedException {

        map1.put("hello1", "world1");
        map2.put("hello2", "world2");
        map3.put("hello3", "world3");

        @Nullable final ModificationIterator iterator1for2 = replicator1.acquireModificationIterator
                (replicator2.identifier());

        @Nullable final ModificationIterator iterator1for3 = replicator1.acquireModificationIterator
                (replicator3.identifier());

        @Nullable final ModificationIterator iterator2for1 = replicator2.acquireModificationIterator
                (replicator1.identifier());

        @Nullable final ModificationIterator iterator2for3 = replicator2.acquireModificationIterator
                (replicator3.identifier());

        @Nullable final ModificationIterator iterator3for1 = replicator3.acquireModificationIterator
                (replicator1.identifier());

        @Nullable final ModificationIterator iterator3for2 = replicator3.acquireModificationIterator
                (replicator2.identifier());

        iterator1for2.dirtyEntries(0);

        while (iterator1for2.hasNext()) {
            iterator1for2.nextEntry(replicator2::applyReplication);
        }
        iterator1for3.dirtyEntries(0);

        while (iterator1for3.hasNext()) {
            iterator1for3.nextEntry(replicator3::applyReplication);
        }
        iterator2for1.dirtyEntries(0);

        while (iterator2for1.hasNext()) {
            iterator2for1.nextEntry(replicator1::applyReplication);
        }
        iterator2for3.dirtyEntries(0);

        while (iterator2for3.hasNext()) {
            iterator2for3.nextEntry(replicator3::applyReplication);
        }
        iterator3for1.dirtyEntries(0);

        while (iterator3for1.hasNext()) {
            iterator3for1.nextEntry(replicator1::applyReplication);
        }
        iterator3for2.dirtyEntries(0);

        while (iterator3for2.hasNext()) {
            iterator3for2.nextEntry(replicator2::applyReplication);
        }

        for (@org.jetbrains.annotations.NotNull Map m : new Map[]{map1, map2, map3}) {
            Assert.assertEquals("world1", m.get("hello1"));
            Assert.assertEquals("world2", m.get("hello2"));
            Assert.assertEquals("world3", m.get("hello3"));
            Assert.assertEquals(3, m.size());
        }
    }
}

