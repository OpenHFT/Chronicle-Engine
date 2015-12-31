package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rob Austin
 */

public class MainClusterClient {
    public static final WireType WIRE_TYPE = WireType.COMPRESSED_BINARY;
    public static final int entries = MainCluster5.entries;
    public static final String NAME = "/ChMaps/test1";

    public static final int VALUE_SIZE = MainCluster5.VALUE_SIZE;
    public static final String DESCRIPTION = System.getProperty("connect", "localhost:8085");


    public static void main(String[] args) throws IOException, InterruptedException {
        YamlLogging.setAll(false);
        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;

        char[] x = new char[VALUE_SIZE - new Random().nextInt(VALUE_SIZE / 10)];
        Arrays.fill(x, 'X');
        final String s = new String(x);

//        Executors.newSingleThreadExecutor().submit(() -> {
        VanillaAssetTree tree5 = new VanillaAssetTree("tree1")
                .forRemoteAccess(DESCRIPTION, WIRE_TYPE, Throwable::printStackTrace);
            final ConcurrentMap<String, String> map1 = tree5.acquireMap(NAME, String.class,
                    String.class);
            for (; ; ) {
                long start = System.currentTimeMillis();
                for (int i = 0; i < entries; i++) {
                    try {
                        map1.put("" + i, s);

                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                map1.size();
                long time = System.currentTimeMillis() - start;
                System.out.println("Send " + entries + " puts in " + time + " ms");
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

