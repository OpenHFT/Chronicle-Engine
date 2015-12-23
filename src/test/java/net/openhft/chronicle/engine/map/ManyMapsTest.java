package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * Created by daniel on 16/07/2015.
 */
public class ManyMapsTest {

    public static final String NAME =
            "ManyMapsTest.testConnectToMultipleMapsUsingTheSamePort.host.port";

    public static String getKey(String mapName, int counter) {
        return String.format("%s-%s", mapName, counter);
    }

    public static String getValue(String mapName, int counter) {
        return String.format("Val-%s-%s", mapName, counter);
    }

    private static AtomicReference<Throwable> t = new AtomicReference();

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) Jvm.rethrow(th);
    }


    @Test
    @Ignore("Long running test")
    public void testConnectToMultipleMapsUsingTheSamePort() throws IOException {
        int noOfMaps = 100;
        int noOfKvps = 100;
        String mapBaseName = "ManyMapsTest-";
        AssetTree assetTree = new VanillaAssetTree().forTesting(x -> t.set(x));

        Map<String, Map<String, String>> _clientMaps = new HashMap<>();
        TCPRegistry.createServerSocketChannelFor(NAME);
        //TODO CHENT-68 Only works with BINARY NOT TEXT.
        ServerEndpoint serverEndpoint = new ServerEndpoint(NAME, assetTree, WireType.BINARY);

        AssetTree clientAssetTree = new VanillaAssetTree().forRemoteAccess(NAME, WireType.BINARY, x -> t.set(x));
        System.out.println("Creating maps.");
        AtomicInteger count = new AtomicInteger();
        IntStream.rangeClosed(1, noOfMaps).forEach(i -> {
            String mapName = mapBaseName + i;

            Map<String, String> map = clientAssetTree.acquireMap(mapName, String.class, String.class);

            for (int j = 1; j <= noOfKvps; j++) {
                map.put(getKey(mapName, j), getValue(mapName, j));
            }

            _clientMaps.put(mapName, map);
            if (count.incrementAndGet() % 100 == 0)
                System.out.print("... " + count);
        });
        System.out.println("...client maps " + noOfMaps + " Done.");

        //Test that the number of maps created exist
        Assert.assertEquals(noOfMaps, _clientMaps.size());

        for (Map.Entry<String, Map<String, String>> mapEntry : _clientMaps.entrySet()) {
            System.out.println(mapEntry.getKey());
            Map<String, String> map = mapEntry.getValue();

            //Test that the number of key-value-pairs in the map matches the expected.
            Assert.assertEquals(noOfKvps, map.size());

            //Test that all the keys in this map contains the map name (ie. no other map's keys overlap).
            String key = mapEntry.getKey();
            SerializablePredicate<String> stringPredicate = k -> !k.contains(key);
            Assert.assertFalse(map.keySet().stream().anyMatch(stringPredicate));

            //Test that all the values in this map contains the map name (ie. no other map's values overlap).
            SerializablePredicate<String> stringPredicate1 = v -> !v.contains(key);
            Assert.assertFalse(map.values().stream().anyMatch(stringPredicate1));
        }
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

}
