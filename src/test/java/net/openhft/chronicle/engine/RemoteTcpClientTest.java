/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.*;
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
import static net.openhft.chronicle.engine.Utils.yamlLoggger;

public class RemoteTcpClientTest extends ThreadMonitoringTest {

    @Rule
    public TestName name = new TestName();

    @Before
    public void before() {
        methodName(name.getMethodName());
    }

    class MyMarshallable implements Marshallable {

        String someData;

        public MyMarshallable(String someData) {
            this.someData = someData;
        }

        @Override
        public void writeMarshallable(WireOut wire) {
            wire.write(() -> "MyField").text(someData);
        }

        @Override
        public void readMarshallable(WireIn wire) throws IllegalStateException {
            someData = wire.read(() -> "MyField").text();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyMarshallable that = (MyMarshallable) o;

            return Objects.equals(someData, that.someData);
        }

        @Override
        public int hashCode() {
            return someData != null ? someData.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MyMarshable{" + "someData='" + someData + '\'' + '}';
        }
    }

    @Test(timeout = 100000)
    @Ignore("performance test")
    public void testLargeStringTextWire() throws Exception {
        final int MB = 1 << 20;
        testStrings(50, 2 * MB, TextWire::new);
    }

    @Test(timeout = 100000)
    @Ignore("performance test")
    public void testLargeStringBinaryWire() throws Exception {
        final int MB = 1 << 20;
        testStrings(50, 2 * MB, BinaryWire::new);
    }

    private void testStrings(int noPutsAndGets, int valueLength, Function<Bytes, Wire> wireType) throws IOException {

        try (final RemoteMapSupplier<CharSequence, CharSequence> remote = new
                RemoteMapSupplier<>(CharSequence.class,
                CharSequence.class,
                BinaryWire::new)) {

            ConcurrentMap test = remote.get();

            Bytes bytes = NativeBytes.nativeBytes(valueLength);
            while (bytes.position() < valueLength)
                bytes.append('x');
            bytes.flip();

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
    public void test2MBEntries() throws Exception {

        // server
        try (final RemoteMapSupplier<String, String> remote = new
                RemoteMapSupplier<>(String.class,
                String.class,
                BinaryWire::new)) {

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
}