package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

/**
 * Created by Rob Austin
 */

public class MainClusterClient {
    public static final WireType WIRE_TYPE = WireType.COMPRESSED_BINARY;
    public static final int entries = MainCluster5.entries;
    public static final String NAME = "/ChMaps/test?entries=" + entries +
            "&averageValueSize=" + MainClusterClient.VALUE_SIZE;

    public static final int VALUE_SIZE = 1 << 20;

    public static void main(String[] args) throws IOException, InterruptedException {
        YamlLogging.clientWrites = false;
        YamlLogging.clientReads = false;
        YamlLogging.setAll(false);
        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;


        char[] x = new char[VALUE_SIZE];
        Arrays.fill(x, 'X');
        final String s = new String(x);

        Executors.newSingleThreadExecutor().submit(() -> {
            VanillaAssetTree tree5 = new VanillaAssetTree("tree1").forRemoteAccess("localhost:8085",
                    WIRE_TYPE);
            final ConcurrentMap<String, String> map1 = tree5.acquireMap(NAME, String.class,
                    String.class);
            for (; ; ) {
                for (int i = 0; i < entries; i++) {
                    try {
                        map1.put("" + i, s);
                        Thread.sleep(20);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }

                }
            }
        });


        Executors.newSingleThreadExecutor().submit(() -> {
            VanillaAssetTree tree5 = new VanillaAssetTree("tree1").forRemoteAccess("localhost:8083",
                    WIRE_TYPE);
            final ConcurrentMap<String, String> map1 = tree5.acquireMap(NAME, String.class,
                    String.class);
            for (; ; ) {
                for (int i = 0; i < entries; i++) {
                    try {
                        map1.remove("" + i);
                        Thread.sleep(20);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }

                }
            }
        });

        YamlLogging.setAll(false);

        final ConcurrentMap<String, String> map;
        AssetTree tree3 = new VanillaAssetTree("tree3").forRemoteAccess("localhost:8083",
                WIRE_TYPE);

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
    }
}

