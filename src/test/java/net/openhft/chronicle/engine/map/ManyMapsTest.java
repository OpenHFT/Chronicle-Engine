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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Created by daniel on 16/07/2015.
 */
public class ManyMapsTest {

    public static final String NAME =
            "ManyMapsTest.testConnectToMultipleMapsUsingTheSamePort.host.port";
    private ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptions;

    public static String getKey(String mapName, int counter) {
        return String.format("%s-%s", mapName, counter);
    }

    public static String getValue(String mapName, int counter) {
        return String.format("Val-%s-%s", mapName, counter);
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void recordExceptions() {
        exceptions = Jvm.recordExceptions();
    }
    @After
    public void afterMethod() {
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    @Test
    public void testConnectToMultipleMapsUsingTheSamePort() throws IOException {
        int noOfMaps = 100;
        int noOfKvps = 100;
        @NotNull String mapBaseName = "ManyMapsTest-";
        @NotNull AssetTree assetTree = new VanillaAssetTree().forTesting();

        @NotNull Map<String, Map<String, String>> _clientMaps = new HashMap<>();
        TCPRegistry.createServerSocketChannelFor(NAME);
        //TODO CHENT-68 Only works with BINARY NOT TEXT.
        @NotNull ServerEndpoint serverEndpoint = new ServerEndpoint(NAME, assetTree);

        @NotNull AssetTree clientAssetTree = new VanillaAssetTree().forRemoteAccess(NAME, WireType.BINARY);
        System.out.println("Creating maps.");
        @NotNull AtomicInteger count = new AtomicInteger();
        IntStream.rangeClosed(1, noOfMaps).forEach(i -> {
            @NotNull String mapName = mapBaseName + i;

            @NotNull Map<String, String> map = clientAssetTree.acquireMap(mapName, String.class, String.class);

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

        for (@NotNull Map.Entry<String, Map<String, String>> mapEntry : _clientMaps.entrySet()) {
            System.out.println(mapEntry.getKey());
            Map<String, String> map = mapEntry.getValue();

            //Test that the number of key-value-pairs in the map matches the expected.
            Assert.assertEquals(noOfKvps, map.size());

            //Test that all the keys in this map contains the map name (ie. no other map's keys overlap).
            String key = mapEntry.getKey();
            @NotNull SerializablePredicate<String> stringPredicate = k -> !k.contains(key);
            Assert.assertFalse(map.keySet().stream().anyMatch(stringPredicate));

            //Test that all the values in this map contains the map name (ie. no other map's values overlap).
            @NotNull SerializablePredicate<String> stringPredicate1 = v -> !v.contains(key);
            Assert.assertFalse(map.values().stream().anyMatch(stringPredicate1));
        }
        clientAssetTree.close();
        assetTree.close();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }
}
