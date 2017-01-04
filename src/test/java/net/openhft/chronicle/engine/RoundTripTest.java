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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.ExceptionKey;
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
    @NotNull
    private static String CONNECTION_1 = "CONNECTION_1";
    @NotNull
    private static String CONNECTION_2 = "CONNECTION_2";
    @NotNull
    private static String CONNECTION_3 = "CONNECTION_3";

    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptions;

    @NotNull
    static AssetTree create(final int hostId, WireType writeType, @NotNull final List<EngineHostDetails> hostDetails) {

        @NotNull AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting();

        @NotNull Map<String, EngineHostDetails> hostDetailsMap = new ConcurrentSkipListMap<>();

        for (@NotNull EngineHostDetails hd : hostDetails) {
            hostDetailsMap.put(hd.toString(), hd);
        }

        @NotNull final Clusters testCluster = new Clusters(Collections.singletonMap("test",
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

        @NotNull Asset asset1 = tree.acquireAsset(SIMPLE_NAME);
        asset1.addView(Clusters.class, testCluster);
        //  VanillaAssetTreeEgMain.registerTextViewofTree("host " + hostId, tree);

        return tree;
    }

    @NotNull
    public static String getKey(int i) {
        return "" + i;
    }

    @NotNull
    public static String generateValue(char c, int size) {
        @NotNull char[] chars = new char[size - 7];
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

    @Before
    public void recordException() {
        exceptions = Jvm.recordExceptions();
    }

    private void checkForThrowablesInOtherThreads() {
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @After
    public void afterMethod() {
        checkForThrowablesInOtherThreads();
    }

    @Test
    @Ignore("Long running")
    public void test() throws IOException, InterruptedException {
        System.out.println("Using cluster " + CLUSTER + " basePath: " + basePath);
        YamlLogging.setAll(false);

        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);
        TCPRegistry.createServerSocketChannelFor(CONNECTION_2);
        TCPRegistry.createServerSocketChannelFor(CONNECTION_3);

        @NotNull final List<EngineHostDetails> hostDetails = new ArrayList<>();
        hostDetails.add(new EngineHostDetails(1, 8 << 20, CONNECTION_1));
        hostDetails.add(new EngineHostDetails(2, 8 << 20, CONNECTION_2));
        hostDetails.add(new EngineHostDetails(3, 8 << 20, CONNECTION_3));

        @NotNull AssetTree serverAssetTree1 = create(1, WireType.BINARY, hostDetails);
        @NotNull AssetTree serverAssetTree2 = create(2, WireType.BINARY, hostDetails);
        @NotNull AssetTree serverAssetTree3 = create(3, WireType.BINARY, hostDetails);

        @NotNull ServerEndpoint serverEndpoint1 = new ServerEndpoint(CONNECTION_1, serverAssetTree1);
        @NotNull ServerEndpoint serverEndpoint2 = new ServerEndpoint(CONNECTION_2, serverAssetTree2);
        @NotNull ServerEndpoint serverEndpoint3 = new ServerEndpoint(CONNECTION_3, serverAssetTree3);

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(basePath, "test"));

        try {
            // configure them
            serverAssetTree1.acquireMap(NAME, String.class, String.class).size();
            serverAssetTree2.acquireMap(NAME, String.class, String.class).size();
            serverAssetTree3.acquireMap(NAME, String.class, String.class).size();

            @NotNull final ConcurrentMap<CharSequence, CharSequence> map1 = serverAssetTree1.acquireMap(NAME, CharSequence.class, CharSequence.class);
            @NotNull final ConcurrentMap<CharSequence, CharSequence> map2 = serverAssetTree2.acquireMap(NAME, CharSequence.class, CharSequence.class);
            @NotNull final ConcurrentMap<CharSequence, CharSequence> map3 = serverAssetTree3.acquireMap(NAME, CharSequence.class, CharSequence.class);

            map1.size();
            map2.size();
            map3.size();

            ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
            ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

            @NotNull CountDownLatch l = new CountDownLatch(ENTRIES * TIMES);

            @NotNull VanillaAssetTree treeC1 = new VanillaAssetTree("tree1")
                    .forRemoteAccess(CONNECTION_1, WIRE_TYPE);

            @NotNull VanillaAssetTree treeC3 = new VanillaAssetTree("tree1")
                    .forRemoteAccess(CONNECTION_3, WIRE_TYPE);
            @NotNull AtomicReference<CountDownLatch> latchRef = new AtomicReference<>();

            treeC3.registerSubscriber(NAME, String.class, z -> {
                latchRef.get().countDown();
            });

            @NotNull final ConcurrentMap<CharSequence, CharSequence> mapC1 = treeC1.acquireMap(NAME, CharSequence.class, CharSequence
                    .class);

            long start = System.currentTimeMillis();

            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;

            @NotNull String[] keys = new String[ENTRIES];
            @NotNull String[] values0 = new String[ENTRIES];
            @NotNull String[] values1 = new String[ENTRIES];
            for (int i = 0; i < ENTRIES; i++) {
                keys[i] = getKey(i);
                values0[i] = generateValue('X', VALUE_SIZE);
                values1[i] = generateValue('-', VALUE_SIZE);
            }
            for (int j = 0; j < TIMES; j++) {
                long timeTakenI = System.currentTimeMillis();
                latchRef.set(new CountDownLatch(ENTRIES));
                @NotNull String[] values = j % 2 == 0 ? values0 : values1;
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

