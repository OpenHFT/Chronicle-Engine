/*
 * Copyright 2014-2018 Chronicle Software
 *
 * http://chronicle.software
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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.EngineInstance;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.ChronicleMapCfg;
import net.openhft.chronicle.engine.cfg.EngineCfg;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.TextWire;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.chronicle.enterprise.map.config.ReplicatedMapCfg;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.engine.tree.VanillaAsset.LAST;
import static org.junit.Assert.assertEquals;

public class MapReplicationTest {

    @Before
    public void setup() throws IOException {
        TCPRegistry.createServerSocketChannelFor("localhost9090", "localhost9091", "localhost90901", "localhost90911");
    }

    @After
    public void tearDown() {
        TCPRegistry.reset();
    }

    @Test
    public void shouldReplicate() throws IOException {
        @NotNull String name = "engineWithMap.yaml";
        VanillaAssetTree host1 = EngineInstance.engineMain(1, name, "cluster");
        VanillaAssetTree host2 = EngineInstance.engineMain(2, name, "cluster");

        ObjectKeyValueStore<String, String> map1 = host1.acquireAsset("/data/map").getView(ObjectKeyValueStore.class);
        map1.put("test", "value");

        ObjectKeyValueStore<String, String> map2 = host2.acquireAsset("/data/map").getView(ObjectKeyValueStore.class);

        map2.put("test2", "value2");
        Jvm.pause(500);
        assertEquals("value", map2.get("test"));
        assertEquals("value2", map1.get("test2"));

        EngineCfg cfg = (EngineCfg) TextWire.fromFile(name).readObject();
        ReplicatedMapCfg mapCfg = (ReplicatedMapCfg) cfg.installableMap.get("/data/map");
        ChronicleMap<String, String> localMap = mapCfg.mapBuilder((byte) 1).createOrRecoverPersistedTo(new File(mapCfg.mapFileDataDirectory() + "/map"));

        localMap.put("anotherTest", "anotherValue");
        System.out.println(localMap.file());

        Jvm.pause(500);

        assertEquals("anotherValue", map1.get("anotherTest"));
        assertEquals("anotherValue", map2.get("anotherTest"));
    }

    @Test
    public void endpointTest() throws IOException {
        @NotNull String name = "engineWithMap.yaml";
        VanillaAssetTree host1 = EngineInstance.engineMain(1, name, "cluster");
        VanillaAssetTree host2 = EngineInstance.engineMain(2, name, "cluster");

        Asset asset = host1.acquireAsset("/manyMaps");
        asset.addLeafRule(AuthenticatedKeyValueStore.class, LAST + " VanillaKeyValueStore", this::createMap);
        asset.addLeafRule(SubscriptionKeyValueStore.class, LAST + " VanillaKeyValueStore", this::createMap);
        asset.addLeafRule(KeyValueStore.class, LAST + " VanillaKeyValueStore", this::createMap);

        asset = host2.acquireAsset("/manyMaps");
        asset.addLeafRule(AuthenticatedKeyValueStore.class, LAST + " VanillaKeyValueStore", this::createMap);
        asset.addLeafRule(SubscriptionKeyValueStore.class, LAST + " VanillaKeyValueStore", this::createMap);
        asset.addLeafRule(KeyValueStore.class, LAST + " VanillaKeyValueStore", this::createMap);

        VanillaAssetTree clientTree = new VanillaAssetTree().forRemoteAccess("localhost9090");
        MapView<String, String> map1 = clientTree.acquireMap("/manyMaps/testMap1", String.class, String.class);

        map1.put("key1", "value1");

        VanillaAssetTree clientTree2 = new VanillaAssetTree().forRemoteAccess("localhost9091");
        MapView<String, String> map2 = clientTree2.acquireMap("/manyMaps/testMap1", String.class, String.class);

        Jvm.pause(500);
        assertEquals("value1", map2.get("key1"));
    }

    private <K, V> AuthenticatedKeyValueStore<K, V> createMap(RequestContext requestContext, Asset asset) {
        return new ChronicleMapKeyValueStore<>(createConfig(requestContext), asset);
    }

    private ChronicleMapCfg createConfig(RequestContext requestContext) {
        ChronicleMapCfg cfg = (ChronicleMapCfg) TextWire.from("!ChronicleMapCfg {\n" +
                "      entries: 10000,\n" +
                "      keyClass: !type String,\n" +
                "      valueClass: !type String,\n" +
                "      exampleKey: \"some_key\",\n" +
                "      exampleValue: \"some_value\",\n" +
                "      mapFileDataDirectory: data/map" + requestContext.basePath() + "Data$hostId,\n" +
                "    }").readObject();

        cfg.name(requestContext.fullName());
        return cfg;
    }
}
