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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;

/**
 * Created by peter on 02/12/15.
 */
public class Main2WayClient {
    public static void main(String[] args) {
        String s = Math.random() < 0.5 ? "localhost:8081, localhost:8082" : "localhost:8082, localhost:8081";
        System.out.println(s);
        ((VanillaAssetTree) Chassis.assetTree()).forRemoteAccess(s.split(", "), Main2Way.WIRE_TYPE);
        MapView<String, String> map = Chassis.acquireMap("/ChMaps/test?putReturnsNull=true", String.class, String.class);
        String data = Main2Way.generateValue();

        while (true) {
            for (int i = 0; i < Main2Way.entries; i += 10) {
                long max = 0;
                for (int j = 0; j < 10; j++) {
                    long start = System.currentTimeMillis();
                    map.put(Main2Way.getKey(i + j), data);
                    map.size();
                    long time = System.currentTimeMillis() - start;
                    Jvm.pause(100);
                    max = Math.max(max, time);
                }
                System.out.println("max: " + max + " ms");
            }
        }
    }
}
