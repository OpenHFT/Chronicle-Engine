/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.*;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Rob Austin
 */

public class RoundTripTest {
    public static final WireType WIRE_TYPE = WireType.BINARY;
    public static final int ENTRIES = 200;
    public static final int TIMES = 10000;
    public static final String basePath = OS.TARGET + '/' + System.getProperty("server", "one");
    public static final String CLUSTER = System.getProperty("cluster", "clusterFive");
    public static final String SIMPLE_NAME = "/ChMaps/test1";
    public static final String NAME = SIMPLE_NAME +
            "?putReturnsNull=true";
    static final int VALUE_SIZE = 2 << 20;
    public static ServerEndpoint serverEndpoint;
    static int counter = 0;
    private static AtomicReference<Throwable> t = new AtomicReference<>();
    private static String CONNECTION_1 = "CONNECTION_1";
    private static String CONNECTION_2 = "CONNECTION_2";
    private static String CONNECTION_3 = "CONNECTION_3";


    private ThreadDump threadDump;

    @NotNull
    static AssetTree create(final int hostId, WireType writeType, final List<EngineHostDetails> hostDetails) {

        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x));

        Map<String, EngineHostDetails> hostDetailsMap = new ConcurrentSkipListMap<>();

        for (EngineHostDetails hd : hostDetails) {
            hostDetailsMap.put(hd.toString(), hd);
        }

        final Clusters testCluster = new Clusters(Collections.singletonMap("test",
                new EngineCluster("test")));

        tree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType)
                        .cluster("test")
                        .entries(ENTRIES)
                        .averageValueSize(VALUE_SIZE),
                        asset));

        Asset asset1 = tree.acquireAsset(SIMPLE_NAME);
        asset1.addView(Clusters.class, testCluster);
        //  VanillaAssetTreeEgMain.registerTextViewofTree("host " + hostId, tree);

        return tree;
    }

    @NotNull
    public static String getKey(int i) {
        return "" + i;
    }

    public static String generateValue(char c, int size) {
        char[] chars = new char[size - 7];
        Arrays.fill(chars, c);
        return counter++ + " " + new String(chars);
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @After
    public void afterMethod() {
        checkForThrowablesInOtherThreads();
    }

    private void checkForThrowablesInOtherThreads() {
        final Throwable th = t.getAndSet(null);
        if (th != null)
            throw Jvm.rethrow(th);
    }

    @Test
    @Ignore("Long running")
    public void test() throws IOException, InterruptedException {
        System.out.println("Using cluster " + CLUSTER + " basePath: " + basePath);
        YamlLogging.setAll(false);

        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);
        TCPRegistry.createServerSocketChannelFor(CONNECTION_2);
        TCPRegistry.createServerSocketChannelFor(CONNECTION_3);

        final List<EngineHostDetails> hostDetails = new ArrayList<>();
        hostDetails.add(new EngineHostDetails(1, 8 << 20, CONNECTION_1));
        hostDetails.add(new EngineHostDetails(2, 8 << 20, CONNECTION_2));
        hostDetails.add(new EngineHostDetails(3, 8 << 20, CONNECTION_3));

        AssetTree serverAssetTree1 = create(1, WireType.BINARY, hostDetails);
        AssetTree serverAssetTree2 = create(2, WireType.BINARY, hostDetails);
        AssetTree serverAssetTree3 = create(3, WireType.BINARY, hostDetails);

        ServerEndpoint serverEndpoint1 = new ServerEndpoint(CONNECTION_1, serverAssetTree1);
        ServerEndpoint serverEndpoint2 = new ServerEndpoint(CONNECTION_2, serverAssetTree2);
        ServerEndpoint serverEndpoint3 = new ServerEndpoint(CONNECTION_3, serverAssetTree3);

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(basePath, "test"));

        try {
            // configure them
            serverAssetTree1.acquireMap(NAME, String.class, String.class).size();
            serverAssetTree2.acquireMap(NAME, String.class, String.class).size();
            serverAssetTree3.acquireMap(NAME, String.class, String.class).size();

            final ConcurrentMap<CharSequence, CharSequence> map1 = serverAssetTree1.acquireMap(NAME, CharSequence.class, CharSequence.class);
            final ConcurrentMap<CharSequence, CharSequence> map2 = serverAssetTree2.acquireMap(NAME, CharSequence.class, CharSequence.class);
            final ConcurrentMap<CharSequence, CharSequence> map3 = serverAssetTree3.acquireMap(NAME, CharSequence.class, CharSequence.class);

            map1.size();
            map2.size();
            map3.size();

            ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
            ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

            CountDownLatch l = new CountDownLatch(ENTRIES * TIMES);

            VanillaAssetTree treeC1 = new VanillaAssetTree("tree1")
                    .forRemoteAccess(CONNECTION_1, WIRE_TYPE, Throwable::printStackTrace);

            VanillaAssetTree treeC3 = new VanillaAssetTree("tree1")
                    .forRemoteAccess(CONNECTION_3, WIRE_TYPE, Throwable::printStackTrace);
            AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

            treeC3.registerSubscriber(NAME, String.class, z -> {
                latchRef.get().countDown();
            });

            final ConcurrentMap<CharSequence, CharSequence> mapC1 = treeC1.acquireMap(NAME, CharSequence.class, CharSequence
                    .class);

            long start = System.currentTimeMillis();

            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            String[] keys = new String[ENTRIES];
            String[] values0 = new String[ENTRIES];
            String[] values1 = new String[ENTRIES];
            for (int i = 0; i < ENTRIES; i++) {
                keys[i] = getKey(i);
                values0[i] = generateValue('X', VALUE_SIZE);
                values1[i] = generateValue('-', VALUE_SIZE);
            }
            for (int j = 0; j < TIMES; j++) {
                long timeTakenI = System.currentTimeMillis();
                latchRef.set(new CountDownLatch(ENTRIES));
                String[] values = j % 2 == 0 ? values0 : values1;
                for (int i = 0; i < ENTRIES; i++) {
                    mapC1.put(keys[i], values[i]);
                }

                while (!latchRef.get().await(1, TimeUnit.MILLISECONDS)) {
                    checkForThrowablesInOtherThreads();
                }

                final long timeTakenIteration = System.currentTimeMillis() -
                        timeTakenI;

                max = Math.max(max, timeTakenIteration);
                min = Math.min(min, timeTakenIteration);
                System.out.println(" - round trip latency=" + timeTakenIteration + "ms");
//                Jvm.pause(10);
            }

            long timeTaken = System.currentTimeMillis() - start;
            final double target = 1000 * TIMES;
            System.out.println("TOTAL round trip latency=" + timeTaken + "ms (target = " +
                    (int) target + "ms) " +
                    "for " + TIMES + "  times. Arg=" + (timeTaken / TIMES) + ", max=" + max + ", " +
                    "min=" + min);

            Assert.assertTrue(timeTaken <= target);

        } finally {
            serverEndpoint1.close();
            serverEndpoint2.close();
            serverEndpoint3.close();
        }

    }
}

