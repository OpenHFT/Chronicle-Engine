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

    public static void main(String[] args) throws IOException, InterruptedException {
        YamlLogging.clientWrites = true;
        YamlLogging.clientReads = true;

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;


        char[] x = new char[1024];
        Arrays.fill(x, 'X');
        final String s = new String(x);
        VanillaAssetTree tree1 = new VanillaAssetTree("tree1").forRemoteAccess("localhost:8083", WIRE_TYPE);
        Executors.newSingleThreadExecutor().submit(() -> {
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
//        AssetTree tree3 = new VanillaAssetTree("tree3").forRemoteAccess("localhost:8083", WIRE_TYPE);

//        map = tree3.acquireMap(NAME, String.class, String.class);


        tree1.registerSubscriber(NAME, MapEvent.class, o ->

        {
            System.out.println((o == null) ? "null" : (o.toString()
                    .length() > 50 ? o.toString().substring(0, 50) : "XXXX"));
        });

        for (; ; ) {
            try {
                Thread.sleep(5000);
            } catch (Exception ignore) {
            }
        }
    }


}

