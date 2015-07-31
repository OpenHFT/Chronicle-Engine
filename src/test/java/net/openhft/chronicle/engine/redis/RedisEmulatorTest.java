package net.openhft.chronicle.engine.redis;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.redis.RedisEmulator.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 31/07/2015.
 */
public class RedisEmulatorTest {
    @Test
    public void testBasicFunctions() throws IOException {
        //For this test we can use a VanillaMapKeyValueStore
        //To test with a ChronicleMapKeyValueStore uncomment lines below
        AssetTree serverAssetTree = new VanillaAssetTree().forTesting();
//        serverAssetTree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
//                VanillaMapView::new, KeyValueStore.class);
//        serverAssetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
//                new ChronicleMapKeyValueStore(context.basePath(OS.TARGET), asset));
        TCPRegistry.createServerSocketChannelFor("RemoteSubscriptionModelPerformanceTest.port");

        ServerEndpoint serverEndpoint = new ServerEndpoint("RemoteSubscriptionModelPerformanceTest.port",
                serverAssetTree, WireType.TEXT);
        AssetTree clientAssetTree = new VanillaAssetTree()
                .forRemoteAccess("RemoteSubscriptionModelPerformanceTest.port", WireType.TEXT);


        MapView myhash = clientAssetTree.acquireMap("/myhash", String.class, String.class);


        //Test for hset
        assertEquals(1, hset(myhash, "field1", "Hello"));
        assertEquals(0, hset(myhash, "field1", "Hello"));

        //Test for dbsize
        assertEquals(1, dbsize(myhash));

        //Test for flushdb
        assertEquals("OK", flushdb(myhash));
        assertEquals(0, dbsize(myhash));

        //Test for hget
        assertEquals(1, hset(myhash, "field1", "foo"));
        assertEquals("foo", hget(myhash, "field1"));
        assertEquals(null, hget(myhash, "field2"));

        //Test for hgetall
        assertEquals("OK", flushdb(myhash));
        assertEquals(1, hset(myhash, "field1", "Hello"));
        assertEquals(1, hset(myhash, "field2", "World"));

        List<String> results = new ArrayList();
        hgetall(myhash, new Consumer<Map.Entry<String, String>>() {
            @Override
            public void accept(Map.Entry<String, String> entry) {
                results.add(entry.getKey());
                results.add(entry.getValue());
            }
        });

        Jvm.pause(100);
        //todo Redis returns the values in the order they were inserted
        assertEquals(4, results.size());
        if(results.get(0).equals("field1")) {
            assertEquals("field1", results.get(0));
            assertEquals("Hello", results.get(1));
            assertEquals("field2", results.get(2));
            assertEquals("World", results.get(3));
        }else if(results.get(0).equals("field2")) {
            assertEquals("field2", results.get(0));
            assertEquals("World", results.get(1));
            assertEquals("field1", results.get(2));
            assertEquals("Hello", results.get(3));
        } else{
            throw new AssertionError("Incorrect results " + results);
        }

        //Test for hdel
        //Test 1 delete
        assertEquals("OK", flushdb(myhash));
        assertEquals(1, RedisEmulator.hset(myhash, "field1", "foo"));
        assertEquals(1, RedisEmulator.hdel(myhash, "field1"));
        assertEquals(0, RedisEmulator.hdel(myhash, "field1"));

        //Test multiple deletes.
//        assertEquals("OK", flushdb(myhash));
//        assertEquals(1, RedisEmulator.hset(myhash, "field1", "foo1"));
//        assertEquals(1, RedisEmulator.hset(myhash, "field2", "foo2"));
//        assertEquals(1, RedisEmulator.hset(myhash, "field3", "foo3"));
//        assertEquals(2, RedisEmulator.hdel(myhash, "field1", "field3"));
    }
}
