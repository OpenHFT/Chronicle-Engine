package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.map.MapEvent;
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
    public static final String NAME = "/ChMaps/test";
    public static final int ENTRY_SIZE = 2 << 20;

    public static void main(String[] args) throws IOException, InterruptedException {
        YamlLogging.clientWrites = true;
        YamlLogging.clientReads = true;

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;


        char[] x = new char[ENTRY_SIZE];
        Arrays.fill(x, 'X');
        final String s = new String(x);

        Executors.newSingleThreadExecutor().submit(() -> {
            VanillaAssetTree tree1 = new VanillaAssetTree("tree1").forRemoteAccess
                    ("localhost:9091", WIRE_TYPE);
            final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class,
                    String.class);
            for (; ; ) {
                for (int i = 0; i < entries; i++) {
                    map1.put("" + i, s);
                    Thread.sleep(2);
                }
            }
        });

        YamlLogging.setAll(false);

        final ConcurrentMap<String, String> map;
//        AssetTree tree3 = new VanillaAssetTree("tree3").forRemoteAccess("localhost:9093", WIRE_TYPE);

//        map = tree3.acquireMap(NAME, String.class, String.class);

        VanillaAssetTree tree3 = new VanillaAssetTree("tree3").forRemoteAccess("localhost:9093",
                WIRE_TYPE);
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

