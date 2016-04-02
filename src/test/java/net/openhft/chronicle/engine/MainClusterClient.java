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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rob Austin
 */

public class MainClusterClient {
    public static final WireType WIRE_TYPE = MainCluster5.WIRE_TYPE;
    public static final int entries = MainCluster5.entries;
    public static final String NAME1 = "/ChMaps/test1?putReturnsNull=true";
    public static final String NAME2 = "/ChMaps/test2?putReturnsNull=true";

    public static final int VALUE_SIZE = MainCluster5.VALUE_SIZE;
    public static final String DESCRIPTION = System.getProperty("connect", "localhost:8085");

    public static void main(String[] args) {
        YamlLogging.setAll(false);
        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");

        char[] xa = new char[VALUE_SIZE - new Random().nextInt(VALUE_SIZE / 10)];
        Arrays.fill(xa, 'X');
        final String x = new String(xa);

        char[] ya = new char[VALUE_SIZE - new Random().nextInt(VALUE_SIZE / 10)];
        Arrays.fill(ya, 'Y');
        final String y = new String(ya);

//        Executors.newSingleThreadExecutor().submit(() -> {
        VanillaAssetTree tree5 = new VanillaAssetTree("tree1")
                .forRemoteAccess(DESCRIPTION, WIRE_TYPE, Throwable::printStackTrace);
        final ConcurrentMap<String, String> map1 = tree5.acquireMap(NAME1,
                String.class, String.class);
        final ConcurrentMap<String, String> map2 = tree5.acquireMap(NAME2,
                String.class, String.class);
        for (int count = 0; ; count++) {
            String v = DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now()) + " - " + (count % 2 == 0 ? x : y);
            long start = System.currentTimeMillis();
            for (int i = 0; i < entries; i++) {
                try {
                    map1.put("" + i, v);
                    map2.put("" + i, v);

                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
            map1.size();
            map2.size();
            long time = System.currentTimeMillis() - start;
            System.out.println("Send " + entries * 2 + " puts in " + time + " ms");
        }
//        });

      /*  Executors.newSingleThreadExecutor().submit(() -> {
            VanillaAssetTree tree5 = new VanillaAssetTree("tree1").forRemoteAccess("localhost:8083",
                    WIRE_TYPE, x -> x.printStackTrace());
            final ConcurrentMap<String, String> map1 = tree5.acquireMap(NAME, String.class,
                    String.class);
            for (; ; ) {
                for (int i = 0; i < entries; i++) {
                    try {
                        map1.remove("" + i);
                       // Thread.sleep(20);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }
        });
*/
/*
        YamlLogging.setAll(false);

        final ConcurrentMap<String, String> map;
        AssetTree tree3 = new VanillaAssetTree("tree3").forRemoteAccess("localhost:8083",
                WIRE_TYPE, z -> z.printStackTrace());

        tree3.acquireMap(NAME, String.class, String.class).size();

        int[] count = {0};
        tree3.registerSubscriber(NAME, MapEvent.class, me -> {
                    System.out.print((me == null) ? "null" : me.getKey());
                    if (++count[0] >= 20) {
                        System.out.println();
                        count[0] = 0;
                    } else {
                        System.out.print("\t");
                    }
                }
        );
        System.in.read();
*/
    }
}

