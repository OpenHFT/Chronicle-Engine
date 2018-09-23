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

import net.openhft.chronicle.engine.EngineInstance;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class ReplicationTest {

    public static void main(String[] args) throws IOException {
        TCPRegistry.createServerSocketChannelFor("localhost9090", "localhost9091", "localhost90901", "localhost90911");
        @NotNull String name = "engineWithMap.yaml";
        VanillaAssetTree host1 = EngineInstance.engineMain(1, name, "cluster");
        VanillaAssetTree host2 = EngineInstance.engineMain(2, name, "cluster");

        ObjectKeyValueStore map1 = host1.acquireAsset("/data/map").getView(ObjectKeyValueStore.class);
        map1.put("test", "value");

        ObjectKeyValueStore map2 = host2.acquireAsset("/data/map").getView(ObjectKeyValueStore.class);
        System.out.println(map2.get("test"));
    }
}
