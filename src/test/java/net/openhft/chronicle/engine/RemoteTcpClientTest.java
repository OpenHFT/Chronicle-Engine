/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
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

    @AfterClass
    public static void tearDownClass() {
//   todo     TCPRegistery.assertAllServersStopped();
        TCPRegistry.reset();
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

    private void testStrings(int noPutsAndGets, int valueLength, WireType wireType) throws IOException {

        try (final RemoteMapSupplier<CharSequence, CharSequence> remote = new
                RemoteMapSupplier<>("testStrings.host.port", CharSequence.class,
                CharSequence.class,
                wireType, assetTree, "test")) {

            ConcurrentMap test = remote.get();

            Bytes bytes = NativeBytes.nativeBytes(valueLength);
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
    @Ignore("Waiting for merge")
    public void test2MBEntries() throws IOException {

        // server
        try (final RemoteMapSupplier<String, String> remote = new
                RemoteMapSupplier<>("test2MBEntries.host.port", String.class,
                String.class,
                WireType.BINARY, assetTree, "test")) {

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 50_000; i++) {
                sb.append('x');
            }

            String value = sb.toString();
            long time = System.currentTimeMillis();
            final ConcurrentMap<String, String> map = remote.get();
            for (int i = 0; i < 2_000; i++) {
                map.put("largeEntry", value);
            }

            System.out.format("Time for 100MB %,dms%n", (System.currentTimeMillis() - time));
        }
    }

    @Test
    @Ignore("Will be very slow, of course")
    public void testLargeUpdates() throws IOException, InterruptedException {
        char[] chars = new char[1024 * 1024];
        Arrays.fill(chars, 'X');
        String value = new String(chars);

        try (final RemoteMapSupplier<String, String> remote =
                     new RemoteMapSupplier<>("testLargeUpdates.host.port",
                             String.class, String.class,
                             WireType.BINARY, assetTree, "test")) {

            List<Closeable> closeables = new ArrayList<>();

            List<Map<String, String>> maps = new ArrayList<>();
            final ConcurrentMap<String, String> map = remote.get();
            maps.add(map);
            for (int i = 1; i < Runtime.getRuntime().availableProcessors(); i++) {
                AssetTree assetTree2 = new VanillaAssetTree().forRemoteAccess("testLargeUpdates.host.port", WireType.BINARY);
                Map<String, String> map2 = assetTree2.acquireMap("test", String.class, String.class);
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
    public void testValuesCollection() throws IOException {

        // server
        try (final RemoteMapSupplier<String, String> remote = new
                RemoteMapSupplier<>("testValuesCollection.host.port", String.class,
                String.class,
                WireType.BINARY, assetTree, "test")) {
            final ConcurrentMap<String, String> map = remote.get();
            HashMap<String, String> data = new HashMap<String, String>();
            data.put("test1", "value1");
            data.put("test1", "value1");
            map.putAll(data);
            assertEquals(data.size(), map.size());
            assertEquals(data.size(), map.values().size());

            Iterator<String> it = map.values().iterator();
            ArrayList<String> values = new ArrayList<String>();
            while (it.hasNext()) {
                values.add(it.next());
            }
            Collections.sort(values);
            Object[] dataValues = data.values().toArray();
            Arrays.sort(dataValues);
            assertArrayEquals(dataValues, values.toArray());
        }
    }
}