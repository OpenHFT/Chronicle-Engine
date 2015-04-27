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

import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.client.RemoteTcpClientChronicleContext;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.map.ChronicleMap;
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
        });
    }


    @Test(timeout = 50000)
    public void testPutAndGet() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {
            mapProxy.put(1, "hello");
            assertEquals("hello", mapProxy.get(1));
            assertEquals(1, mapProxy.size());
        });
    }



    @Test(timeout = 50000)
    public void testEntrySetIsEmpty() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {
            assertEquals(true, mapProxy.isEmpty());
        });

    }


    @Test(timeout = 50000)
    public void testEntrySetRemove() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {
            final Set<Map.Entry<Integer, String>> entries = mapProxy.entrySet();

            assertEquals(true, entries.isEmpty());
            mapProxy.put(1, "hello");

            assertEquals(false, entries.isEmpty());
            entries.remove(1);

            assertEquals(true, entries.isEmpty());
        });

    }


    @Test(timeout = 50000)
    public void testPutAll() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {


            final Set<Map.Entry<Integer, String>> entries = mapProxy.entrySet();

            assertEquals(true, entries.isEmpty());

            Map<Integer, String> data = new HashMap<>();
            data.put(1, "hello");
            data.put(2, "world");

            assertEquals(true, entries.isEmpty());
            mapProxy.putAll(data);
            assertEquals(2, entries.size());

        });
    }


   public interface CloseableSupplier<X> extends Closeable, Supplier<X> {
    }

    static class RemoteMapSupplier<K, V> implements CloseableSupplier<ChronicleMap<K, V>> {

        final ServerEndpoint serverEndpoint;
        private final ChronicleMap<K, V> map;
        private final RemoteTcpClientChronicleContext context;

        public RemoteMapSupplier(Class<K> kClass, Class<V> vClass) throws IOException {

            serverEndpoint = new ServerEndpoint((byte) 1);
            int serverPort = serverEndpoint.getPort();

            context = new RemoteTcpClientChronicleContext("localhost", serverPort, (byte) 2);
            map = context.getMap("test", kClass, vClass);
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


    public static class LocalMapSupplier<K, V> implements CloseableSupplier<ChronicleMap<K, V>> {

        private final ChronicleMap<K, V> map;
        private final ChronicleEngine context;

        public LocalMapSupplier(Class<K> kClass, Class<V> vClass) throws IOException {
            context = new ChronicleEngine();
            map = context.getMap("test", kClass, vClass);
        }


        @Override
        public void close() throws IOException {
            context.close();
        }


        @Override
        public ChronicleMap<K, V> get() {
            return map;
        }


    }

    /**
     * supplies a map and closes it once the tests are finished
     */
    private <K, V>
    void supplyMap(Class<K> kClass, Class<V> vClass, Consumer<ConcurrentMap<K, V>> c)
            throws IOException {

        CloseableSupplier<ChronicleMap<K, V>> result;
        if (LocalMapSupplier.class.equals(supplier)) {
            result = new LocalMapSupplier<K, V>(kClass, vClass);

        } else if (RemoteMapSupplier.class.equals(supplier)) {
            result = new RemoteMapSupplier<K, V>(kClass, vClass);

        } else {
            throw new IllegalStateException("unsuported type");
        }

        final ConcurrentMap<K, V> kvMap = result.get();
        try {
            c.accept(kvMap);
        } finally {
            result.close();
        }

    }

}




