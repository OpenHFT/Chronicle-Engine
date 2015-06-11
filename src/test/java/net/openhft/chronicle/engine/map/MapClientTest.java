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
import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static net.openhft.chronicle.engine.api.WireType.wire;
import static org.junit.Assert.assertEquals;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class MapClientTest extends ThreadMonitoringTest {

    public static int i;
    // server has it's own asset tree, to the client.
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    @Nullable
    private Class<? extends CloseableSupplier> supplier = null;

    public MapClientTest(Class<? extends CloseableSupplier> supplier) {
        this.supplier = supplier;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        return Arrays.asList(new Class[][]{
                {LocalMapSupplier.class},
                // {RemoteMapSupplier.class}
        });
    }

    @Test(timeout = 50000)
    public void testPutAndGet() throws IOException, InterruptedException {
        yamlLoggger(() -> {
            try {
                supplyMap(Integer.class, String.class, mapProxy -> {

                    mapProxy.put(1, "hello");
                    assertEquals("hello", mapProxy.get(1));
                    assertEquals(1, mapProxy.size());

                    Assert.assertEquals("{1=hello}", mapProxy.toString());

                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

/*    @Test(timeout = 50000)
    public void testSubscriptionTest() throws IOException, InterruptedException {
        yamlLoggger(() -> {
            try {
                supplyMap(Integer.class, String.class, map -> {
                    try {
                        supplyMapEventListener(Integer.class, String.class, mapEventListener -> {
                            Chassis.registerSubscriber("test", ChangeEvent.class, e -> e.apply(mapEventListener));

                            map.put(i, "one");

                        });

                    } catch (IOException e) {
                        Jvm.rethrow(e);
                    }
                });

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }*/

    @Test(timeout = 50000)
    public void testEntrySetIsEmpty() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {
            assertEquals(true, mapProxy.isEmpty());
        });
    }

    @Test
    public void testPutAll() throws IOException, InterruptedException {

        supplyMap(Integer.class, String.class, mapProxy -> {

            yamlLoggger(() -> {
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
        });
    }

    @Test
    public void testMapsAsValues() throws IOException, InterruptedException {

        supplyMap(Integer.class, Map.class, map -> {

            {
                final Map value = new HashMap<String, String>();
                value.put("k1", "v1");
                value.put("k2", "v2");

                map.put(1, value);
            }

            {
                final Map value = new HashMap<String, String>();
                value.put("k3", "v3");
                value.put("k4", "v4");

                map.put(2, value);
            }

            assertEquals("v1", map.get(1).get("k1"));
            assertEquals("v2", map.get(1).get("k2"));

            assertEquals(null, map.get(1).get("k3"));
            assertEquals(null, map.get(1).get("k4"));

            assertEquals("v3", map.get(2).get("k3"));
            assertEquals("v4", map.get(2).get("k4"));

            assertEquals(2, map.size());
        });
    }

    @Test
    public void testDoubleValues() throws IOException, InterruptedException {

        supplyMap(Double.class, Double.class, mapProxy -> {

            mapProxy.put(1.0, 1.0);
            mapProxy.put(2.0, 2.0);
            assertEquals(1.0, mapProxy.get(1.0), 0);
            assertEquals(2.0, mapProxy.get(2.0), 0);

            assertEquals(2, mapProxy.size());
        });
    }

    @Test
    public void testFloatValues() throws IOException, InterruptedException {

        supplyMap(Float.class, Float.class, mapProxy -> {

            mapProxy.put(1.0f, 1.0f);
            mapProxy.put(2.0f, 2.0f);
            assertEquals(1.0f, mapProxy.get(1.0f), 0);
            assertEquals(2.0f, mapProxy.get(2.0f), 0);

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

    /**
     * supplies a listener and closes it once the tests are finished
     */
    private <K, V>
    void supplyMap(@NotNull Class<K> kClass, @NotNull Class<V> vClass, @NotNull Consumer<ConcurrentMap<K, V>> c)
            throws IOException {

        CloseableSupplier<ConcurrentMap<K, V>> result;
        if (LocalMapSupplier.class.equals(supplier)) {
            result = new LocalMapSupplier<K, V>(kClass, vClass);

        } else if (RemoteMapSupplier.class.equals(supplier)) {
            result = new RemoteMapSupplier<K, V>(kClass, vClass, TextWire::new, assetTree);

        } else {
            throw new IllegalStateException("unsuported type");
        }

        try {
            c.accept(result.get());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            result.close();
        }
    }

    public interface CloseableSupplier<X> extends Closeable, Supplier<X> {
    }

    public static class RemoteMapSupplier<K, V> implements CloseableSupplier<ConcurrentMap<K, V>> {

        @NotNull
        final ServerEndpoint serverEndpoint;
        @NotNull
        private final ConcurrentMap<K, V> map;

        public RemoteMapSupplier(@NotNull final Class<K> kClass,
                                 @NotNull final Class<V> vClass,
                                 @NotNull final Function<Bytes, Wire> wireType,
                                 AssetTree assetTree,
                                 String name) throws IOException {
            wire = wireType;

            serverEndpoint = new ServerEndpoint(assetTree);
            int serverPort = serverEndpoint.getPort();

            final String hostname = "localhost";

            Chassis.forRemoteAccess();

            map = assetTree.acquireMap(
                    toUri(name,serverPort, hostname),
                    kClass,
                    vClass);
        }

        public RemoteMapSupplier(@NotNull final Class<K> kClass,
                                 @NotNull final Class<V> vClass,
                                 @NotNull final Function<Bytes, Wire> wireType,
                                 AssetTree assetTree) throws IOException {
            this(kClass,vClass,wireType,assetTree,"test");
        }

        @NotNull
        public static String toUri(final String name, final long serverPort, final String hostname) {
            return name + "?port=" + serverPort +
                    "&host=" + hostname +
                    "&timeout=1000";
        }

        @Override
        public void close() throws IOException {
            if (map instanceof Closeable)
                ((Closeable) map).close();
            Chassis.close();
            serverEndpoint.close();
        }

        @NotNull
        @Override
        public ConcurrentMap<K, V> get() {
            return map;
        }
    }
    public static class LocalMapSupplier<K, V> implements CloseableSupplier<ConcurrentMap<K, V>> {

        @NotNull
        private final ConcurrentMap<K, V> map;

        public LocalMapSupplier(Class<K> kClass, Class<V> vClass) throws IOException {
            map = Chassis.acquireMap("test" + i++ + "?putReturnsNull=false&removeReturnsNull=false", kClass, vClass);
        }

        @Override
        public void close() throws IOException {
            if (map instanceof Closeable)
                ((Closeable) map).close();
        }

        @NotNull
        @Override
        public ConcurrentMap<K, V> get() {
            return map;
        }
    }
}

