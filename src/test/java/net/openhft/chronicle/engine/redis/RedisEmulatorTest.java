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

package net.openhft.chronicle.engine.redis;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.redis.RedisEmulator.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 31/07/2015.
 */
public class RedisEmulatorTest {
    private static MapView myStringHash;
    private static MapView myLongHash;
    private static MapView myDoubleHash;

    private static AtomicReference<Throwable> t = new AtomicReference();

    @BeforeClass
    public static void setup() throws IOException{
        System.out.println("Hello");
        YamlLogging.showServerReads(true);
        //For this test we can use a VanillaMapKeyValueStore
        //To test with a ChronicleMapKeyValueStore uncomment lines below
        AssetTree serverAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
//        serverAssetTree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
//                VanillaMapView::new, KeyValueStore.class);
//        serverAssetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
//                new ChronicleMapKeyValueStore(context.basePath(OS.TARGET), asset));
        TCPRegistry.createServerSocketChannelFor("RemoteSubscriptionModelPerformanceTest.port");

        ServerEndpoint serverEndpoint = new ServerEndpoint("RemoteSubscriptionModelPerformanceTest.port",
                serverAssetTree);
        AssetTree clientAssetTree = new VanillaAssetTree()
                .forRemoteAccess("RemoteSubscriptionModelPerformanceTest.port", WireType.TEXT, x -> t.set(x));

        myStringHash = clientAssetTree.acquireMap("/myStringHash", String.class, String.class);
        myLongHash = clientAssetTree.acquireMap("/myLongHash", String.class, Long.class);
        myDoubleHash = clientAssetTree.acquireMap("/myDoubleHash", String.class, Double.class);
    }

    @AfterClass
    public static void down() throws IOException {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Before
    public void before(){
        assertEquals("OK", flushdb(myStringHash));
        assertEquals("OK", flushdb(myLongHash));
    }

    @Test
    public void test_dbsize () throws IOException{
        assertEquals(0, dbsize(myStringHash));
        assertEquals(1, hset(myStringHash, "field1", "Hello"));
        assertEquals(1, hset(myStringHash, "field2", "World"));
        assertEquals(2, dbsize(myStringHash));
    }

    @Test
    public void test_flushdb() {
        assertEquals("OK", flushdb(myStringHash));
        assertEquals(0, dbsize(myStringHash));
    }

    @Test
    public void test_hget() {
        assertEquals(1, hset(myStringHash, "field1", "foo"));
        assertEquals("foo", hget(myStringHash, "field1"));
        assertEquals(null, hget(myStringHash, "field2"));
    }

    @Test
    public void test_hgetall() {
        assertEquals(1, hset(myStringHash, "field1", "Hello"));
        assertEquals(1, hset(myStringHash, "field2", "World"));

        List<String> results = new ArrayList();
        hgetall(myStringHash, new Consumer<Map.Entry<String, String>>() {
            @Override
            public void accept(Map.Entry<String, String> entry) {
                results.add(entry.getKey());
                results.add(entry.getValue());
            }
        });

        Jvm.pause(100);
        //todo Redis returns the values in the order they were inserted
        assertEquals(4, results.size());
        if (results.get(0).equals("field1")) {
            assertEquals("field1", results.get(0));
            assertEquals("Hello", results.get(1));
            assertEquals("field2", results.get(2));
            assertEquals("World", results.get(3));
        } else if (results.get(0).equals("field2")) {
            assertEquals("field2", results.get(0));
            assertEquals("World", results.get(1));
            assertEquals("field1", results.get(2));
            assertEquals("Hello", results.get(3));
        } else {
            throw new AssertionError("Incorrect results " + results);
        }
    }

    @Test
    public void test_hdell_single() {
        assertEquals(1, hset(myStringHash, "field1", "foo"));
        assertEquals(1, hdel(myStringHash, "field1"));
        assertEquals(0, hdel(myStringHash, "field1"));
    }

    @Test
    public void test_hdell_multiple() {
        assertEquals(1, hset(myStringHash, "field1", "foo1"));
        assertEquals(1, hset(myStringHash, "field2", "foo2"));
        assertEquals(1, hset(myStringHash, "field3", "foo3"));
        assertEquals(2, hdel(myStringHash, "field1", "field3"));
    }

    @Test
    public void test_hexists() {
        assertEquals(1, hset(myStringHash, "field1", "foo"));
        assertEquals(1, hexists(myStringHash, "field1"));
        assertEquals(0, hexists(myStringHash, "field2"));
    }

    @Test
    public void test_hset() {
        assertEquals(1, hset(myStringHash, "field1", "Hello"));
        assertEquals(0, hset(myStringHash, "field1", "Hello"));
    }

    @Test
    @Ignore //WIRE-29 maps can't be serialised
    public void test_hmget(){
        assertEquals(1, hset(myStringHash, "field1", "Hello"));
        assertEquals(1, hset(myStringHash, "field2", "World"));
        Map hmget = hmget(myStringHash, "field1", "field2", "nofield");
        System.out.println(hmget);
    }

    @Test
     public void test_exists_single() {
        assertEquals(1, hset(myStringHash, "key1", "Hello"));
        assertEquals(0, exists(myStringHash, "nosuchkey"));
        assertEquals(1, hset(myStringHash, "key2", "World"));
    }

    @Test
    public void test_exists_multiple() {
        assertEquals(1, hset(myStringHash, "key1", "Hello"));
        assertEquals(1, hset(myStringHash, "key2", "World"));
        assertEquals(2, exists(myStringHash, "key1", "key2", "nosuchkey"));
    }

    @Test
    public void test_incr() {
        assertEquals(1, hset(myLongHash, "mykey", 10));
        assertEquals(11, incr(myLongHash, "mykey"));
    }

    @Test
    public void test_incrby() {
        assertEquals(1, hset(myLongHash, "mykey", 10l));
        assertEquals(15, incrby(myLongHash, "mykey", 5l));
    }

    @Test
    public void test_incrbyfloat() {
        assertEquals(1, hset(myDoubleHash, "mykey", 10.5));
        assertEquals(10.5, hget(myDoubleHash, "mykey"));
        assertEquals(10.6, incrbyfloat(myDoubleHash, "mykey", 0.1),0);
        assertEquals(0, hset(myDoubleHash, "mykey", 5.0e3));
        assertEquals(5200, incrbyfloat(myDoubleHash, "mykey", 2.0e2),0);
    }

    @Test
    public void test_append() {
        assertEquals(0, exists(myStringHash, "mykey"));
        assertEquals(5, append(myStringHash, "mykey", "Hello"));
        assertEquals(11, append(myStringHash, "mykey", " World"));
        assertEquals("Hello World", get(myStringHash, "mykey"));
    }

    @Test
    @Ignore //NPE
    public void test_lpush(){
        assertEquals(1, lpush(myStringHash, "mylist", "world"));
        assertEquals(2, lpush(myStringHash, "mylist", "hello"));
        assertEquals(2, lrange(myStringHash, "mylist", 0, -1));
    }
}
