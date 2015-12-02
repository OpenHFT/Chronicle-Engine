/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        ((VanillaAssetTree) Chassis.assetTree()).forRemoteAccess("localhost:8081, localhost:8082".split(", "), Main2Way.WIRE_TYPE);
        MapView<String, String> map = Chassis.acquireMap("/ChMaps/test?putReturnsNull=true", String.class, String.class);
        String data = Main2Way.generateValue();

        while (true) {
            for (int i = 0; i < Main2Way.entries; i += 10) {
                long max = 0;
                for (int j = 0; j < 10; j++) {
                    long start = System.currentTimeMillis();
                    map.put(Main2Way.getKey(i + j), data);
                    System.out.println("Size: " + map.size());
                    long time = System.currentTimeMillis() - start;
                    Jvm.pause(100);
                    max = Math.max(max, time);
                }
                System.out.println("max: " + max + " ms");
            }
        }
    }
}
