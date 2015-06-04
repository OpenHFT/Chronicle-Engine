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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.client.RemoteChassis;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
/**
 * test using the map both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)

public class MapClientTest extends ThreadMonitoringTest {

    private static final Logger LOG = LoggerFactory.getLogger(MapClientTest.class);
    private Class<? extends CloseableSupplier> supplier = null;

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        return Arrays.asList(new Class[][]{
                {LocalMapSupplier.class},
                {RemoteMapSupplier.class}
        });
    }

    public MapClientTest(Class<? extends CloseableSupplier> supplier) {
        this.supplier = supplier;
    }

    @Test(timeout = 50000)
    public void testPutAndGet() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {
            mapProxy.put(1, "hello");
            assertEquals("hello", mapProxy.get(1));
            assertEquals(1, mapProxy.size());

            Assert.assertEquals("{1=hello}", mapProxy.toString());
        });
    }

    @Test(timeout = 50000)
    public void testEntrySetIsEmpty() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {
            assertEquals(true, mapProxy.isEmpty());
        });
    }

    @Test
    public void testPutAll() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {

            final Set<Map.Entry<Integer, String>> entries = mapProxy.entrySet();

            assertEquals(0, entries.size());
            assertEquals(true, entries.isEmpty());

            Map<Integer, String> data = new HashMap<>();
            data.put(1, "hello");
            data.put(2, "world");
            mapProxy.putAll(data);

            final Set<Map.Entry<Integer, String>> e = mapProxy.entrySet();
            final Iterator<Map.Entry<Integer, String>> iterator = e.iterator();
            Map.Entry<Integer, String> entry = iterator.next();

            if (entry.getKey() == 1) {
                assertEquals("hello", entry.getValue());
                entry = iterator.next();
                assertEquals("world", entry.getValue());

            } else if (entry.getKey() == 2) {
                assertEquals("world", entry.getValue());
                entry = iterator.next();
                assertEquals("hello", entry.getValue());
            }


            assertEquals(2, mapProxy.size());
        });
    }

    @Ignore("Unsupported type :!!seqmap[")
    @Test
    public void testMapsAsValues() throws IOException, InterruptedException {

        supplyMap(Integer.class, Map.class, mapProxy -> {

            final Map value = new HashMap<String, String>();
            {
                value.put("k1", "v1");
                value.put("k2", "v2");

                mapProxy.put(1, value);
            }

            {
                value.put("k3", "v3");
                value.put("k4", "v4");

                mapProxy.put(2, value);
            }

            final Object k1 = mapProxy.get(1);
            assertEquals("v2", mapProxy.get(1).get("k2"));

            assertEquals(null, mapProxy.get(1).get("k3"));
            assertEquals(null, mapProxy.get(1).get("k4"));

            assertEquals("v3", mapProxy.get(2).get("k3"));
            assertEquals("v4", mapProxy.get(2).get("k4"));

            assertEquals(2, mapProxy.size());
        });
    }

    @Test
    public void testStringString() throws IOException, InterruptedException {

        supplyMap(String.class, String.class, mapProxy -> {
            mapProxy.put("hello", "world");
            Assert.assertEquals("world", mapProxy.get("hello"));
            assertEquals(1, mapProxy.size());
        });
    }

    @Test
    public void testToString() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {

            mapProxy.put(1, "Hello");
            Assert.assertEquals("Hello", mapProxy.get(1));
            Assert.assertEquals("{1=Hello}", mapProxy.toString());
            mapProxy.remove(1);

            mapProxy.put(2, "World");
            Assert.assertEquals("{2=World}", mapProxy.toString());
        });
    }

    public interface CloseableSupplier<X> extends Closeable, Supplier<X> {
    }

    public static class RemoteMapSupplier<K, V> implements CloseableSupplier<ConcurrentMap<K, V>> {

        final ServerEndpoint serverEndpoint;
        private final ChronicleMap<K, V> map;
        private final RemoteChassis context;

        public RemoteMapSupplier(@NotNull final Class<K> kClass,
                                 @NotNull final Class<V> vClass,
                                 @NotNull final Function<Bytes, Wire> wireType) throws IOException {

            serverEndpoint = new ServerEndpoint(wireType);
            int serverPort = serverEndpoint.getPort();

            context = new RemoteChassis("localhost", serverPort, (byte) 2, wireType);
            map = context.acquireMap("test" + i++, kClass, vClass);
        }

        @Override
        public void close() throws IOException {
            if (map != null)
                map.close();
            context.close();
            serverEndpoint.close();
        }

        @Override
        public ChronicleMap<K, V> get() {
            return map;
        }

    }

    public static int i;

    public static class LocalMapSupplier<K, V> implements CloseableSupplier<ConcurrentMap<K, V>> {

        private final ConcurrentMap<K, V> map;

        public LocalMapSupplier(Class<K> kClass, Class<V> vClass) throws IOException {
            map = Chassis.acquireMap("test" + i++, kClass, vClass);
        }

        @Override
        public void close() throws IOException {
            //    context.close();
        }

        @Override
        public ConcurrentMap<K, V> get() {
            return map;
        }

    }

    /**
     * supplies a map and closes it once the tests are finished
     */
    private <K, V>
    void supplyMap(Class<K> kClass, Class<V> vClass, Consumer<ConcurrentMap<K, V>> c)
            throws IOException {

        CloseableSupplier<ConcurrentMap<K, V>> result;
        if (LocalMapSupplier.class.equals(supplier)) {
            result = new LocalMapSupplier<K, V>(kClass, vClass);

        } else if (RemoteMapSupplier.class.equals(supplier)) {
            result = new RemoteMapSupplier<K, V>(kClass, vClass, TextWire::new);

        } else {
            throw new IllegalStateException("unsuported type");
        }

        final ConcurrentMap<K, V> kvMap = result.get();
        try {
            c.accept(result.get());
        } finally {
            result.close();
        }

    }
}

