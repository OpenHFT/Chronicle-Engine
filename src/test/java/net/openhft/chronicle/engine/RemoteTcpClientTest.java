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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RemoteTcpClientTest extends ThreadMonitoringTest {

    @NotNull
    @Rule
    public TestName name = new TestName();
    @NotNull
    private AssetTree assetTree = new VanillaAssetTree().forTesting();

    @Before
    public void before() {
        methodName(name.getMethodName());
    }

    @Test(timeout = 100000)
    @Ignore("performance test")
    public void testLargeStringTextWire() throws IOException {
        final int MB = 1 << 20;
        testStrings(50, 2 * MB, WireType.TEXT);
    }

    @Test(timeout = 100000)
    @Ignore("performance test")
    public void testLargeStringBinaryWire() throws IOException {
        final int MB = 1 << 20;
        testStrings(50, 2 * MB, WireType.BINARY);
    }

    private void testStrings(int noPutsAndGets, int valueLength, @NotNull WireType wireType) throws IOException {

        try (@NotNull final RemoteMapSupplier<CharSequence, CharSequence> remote = new
                RemoteMapSupplier<>("testStrings.host.port", CharSequence.class,
                CharSequence.class,
                wireType, assetTree, "test")) {

            @NotNull ConcurrentMap test = remote.get();

            @NotNull Bytes bytes = Bytes.allocateElasticDirect(valueLength);
            while (bytes.readPosition() < valueLength)
                bytes.append('x');

            // warm up
            for (int j = -1; j < 30; j++) {
                long start1 = System.currentTimeMillis();
                // TODO adding .parallel() should work.
                IntStream.range(0, noPutsAndGets).parallel().forEach(i -> {
//                    IntStream.range(0, noPutsAndGets).forEach(i -> {
                    test.put("key" + i, bytes);
                    if (i % 10 == 5)
                        System.out.println("put key" + i);
                });
                long duration1 = System.currentTimeMillis() - start1;
                   /* if (j >= 0)*/
                {
                    System.out.printf("Took %.3f seconds to perform %,d puts%n", duration1 / 1e3, noPutsAndGets);
//                        Assert.assertTrue("This should take 1 second but took " + duration1 / 1e3 + " seconds. ", duration1 < 1000);
                }

/*                    long start2 = System.currentTimeMillis();

//                    IntStream.range(0, noPutsAndGets).parallel().forEach(i -> {
                    IntStream.range(0, noPutsAndGets).forEach(i -> {
                        test.getUsing("key" + i, Wires.acquireStringBuilder());
                        if (i % 10 == 5)
                            System.out.println("get key" + i);
                    });
                    long duration2 = System.currentTimeMillis() - start2;
                    if (j >= 0) {
                        System.out.printf("Took %.3f seconds to perform %,d puts%n", duration2 / 1e3, noPutsAndGets);
                        Assert.assertTrue("This should take 1 second but took " + duration2 / 1e3 + " seconds. ", duration2 < 1000);
                    }*/
            }
        }
    }

    @Test
    @Ignore("Takes 407s (~0.2s/iteration) so best to only run it on demand")
    public void test2MBEntries() throws IOException {

        // server
        try (@NotNull final RemoteMapSupplier<String, String> remote = new
                RemoteMapSupplier<>("test2MBEntries.host.port", String.class,
                String.class,
                WireType.BINARY, assetTree, "test")) {

            @NotNull StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 50_000; i++) {
                sb.append('x');
            }

            @NotNull String value = sb.toString();
            long time = System.currentTimeMillis();
            @NotNull final ConcurrentMap<String, String> map = remote.get();
            for (int i = 0; i < 2_000; i++) {
                System.out.println(i);
                map.put("largeEntry", value);
            }

            System.out.format("Time for 100MB %,dms%n", (System.currentTimeMillis() - time));
        }
    }

    @Test
    @Ignore("Will be very slow, of course")
    public void testLargeUpdates() throws IOException, InterruptedException {
        @NotNull char[] chars = new char[1024 * 1024];
        Arrays.fill(chars, 'X');
        @NotNull String value = new String(chars);

        try (@NotNull final RemoteMapSupplier<String, String> remote =
                     new RemoteMapSupplier<>("testLargeUpdates.host.port",
                             String.class, String.class,
                             WireType.BINARY, assetTree, "test")) {

            @NotNull List<Closeable> closeables = new ArrayList<>();

            @NotNull List<Map<String, String>> maps = new ArrayList<>();
            @NotNull final ConcurrentMap<String, String> map = remote.get();
            maps.add(map);
            for (int i = 1; i < Runtime.getRuntime().availableProcessors(); i++) {
                @NotNull AssetTree assetTree2 = new VanillaAssetTree().forRemoteAccess("testLargeUpdates.host.port", WireType.BINARY);
                @NotNull Map<String, String> map2 = assetTree2.acquireMap("test", String.class, String.class);
                maps.add(map2);
                closeables.add(assetTree2);
            }

            final long time = System.currentTimeMillis();
            for (int j = 0; j <= 30 * 1000; j += maps.size() * 100) {
                System.out.println(j);
                maps.parallelStream().forEach(m -> IntStream.range(0, 100).forEach(i -> m.put("key" + i, value)));
            }

            System.out.format("Time for 100MB %,dms%n", (System.currentTimeMillis() - time));
            closeables.forEach(Closeable::close);
        }
    }

    @Test
    @Ignore("Works stand a lone but not when running all tests in IDEA")
    public void testValuesCollection() throws IOException {

        // server
        try (@NotNull final RemoteMapSupplier<String, String> remote = new
                RemoteMapSupplier<>("testValuesCollection.host.port", String.class,
                String.class,
                WireType.BINARY, assetTree, "test")) {
            @NotNull final MapView<String, String> map = remote.get();
            @NotNull Map<String, String> data = new HashMap<String, String>();
            data.put("test1", "value1");
            data.put("test2", "value2");
            map.putAll(data);
            Utils.waitFor(() -> data.size() == map.size());
            assertEquals(data.size(), map.size());
            assertEquals(data.size(), map.values().size());

            @NotNull Iterator<String> it = map.values().iterator();
            @NotNull ArrayList<String> values = new ArrayList<String>();
            while (it.hasNext()) {
                values.add(it.next());
            }
            Collections.sort(values);
            @NotNull Object[] dataValues = data.values().toArray();
            Arrays.sort(dataValues);
            assertArrayEquals(dataValues, values.toArray());

/*    MapView<String, ?, String> map = acquireMap("my-map", String.class, String.class);

    Set<String> set =...
    Map<String, String> subset = map.applyTo(m -> {
        Map<String, String> ret = new HashMap<String, String>();
        for (String key : set) {
            ret.put(key, m.get(key));
        }
        return ret;
    });*/
        }
    }
}