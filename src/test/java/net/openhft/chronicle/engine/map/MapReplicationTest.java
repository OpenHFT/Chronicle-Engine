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
}
