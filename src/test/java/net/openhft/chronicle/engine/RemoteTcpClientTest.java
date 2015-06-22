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
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.IntStream;

import static net.openhft.chronicle.engine.Utils.methodName;

public class RemoteTcpClientTest extends ThreadMonitoringTest {

    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();

    @Before
    public void before() {
        methodName(name.getMethodName());
    }

    @Test(timeout = 100000)
    @Ignore("performance test")
    public void testLargeStringTextWire() throws Exception {
        final int MB = 1 << 20;
        testStrings(50, 2 * MB, WireType.TEXT);
    }

    @Test(timeout = 100000)
    @Ignore("performance test")
    public void testLargeStringBinaryWire() throws Exception {
        final int MB = 1 << 20;
        testStrings(50, 2 * MB, WireType.BINARY);
    }

    private void testStrings(int noPutsAndGets, int valueLength, Function<Bytes, Wire> wireType) throws IOException {

        try (final RemoteMapSupplier<CharSequence, CharSequence> remote = new
                RemoteMapSupplier<>(CharSequence.class,
                CharSequence.class,
                WireType.BINARY, assetTree)) {

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
    public void test2MBEntries() throws Exception {

        // server
        try (final RemoteMapSupplier<String, String> remote = new
                RemoteMapSupplier<>(String.class,
                String.class,
                WireType.BINARY, assetTree)) {

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

    class MyMarshallable implements Marshallable {

        String someData;

        public MyMarshallable(String someData) {
            this.someData = someData;
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wire) {
            wire.write(() -> "MyField").text(someData);
        }

        @Override
        public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
            someData = wire.read(() -> "MyField").text();
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyMarshallable that = (MyMarshallable) o;

            return Objects.equals(someData, that.someData);
        }

        @Override
        public int hashCode() {
            return someData != null ? someData.hashCode() : 0;
        }

        @NotNull
        @Override
        public String toString() {
            return "MyMarshable{" + "someData='" + someData + '\'' + '}';
        }
    }
}