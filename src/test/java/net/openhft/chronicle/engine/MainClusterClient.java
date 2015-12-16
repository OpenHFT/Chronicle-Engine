package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

/**
 * Created by Rob Austin
 */

public class MainClusterClient {
    public static final WireType WIRE_TYPE = WireType.COMPRESSED_BINARY;
    public static final int entries = 10;
    public static final String NAME = "/ChMaps/test?entries=" + entries + "&averageValueSize=" + (2 << 20);


    public static void main(String[] args) throws IOException, InterruptedException {
        YamlLogging.clientWrites = true;
        YamlLogging.clientReads = true;

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        WireType writeType = WireType.BINARY;


        char[] x = new char[1 << 20];
        Arrays.fill(x, 'X');
        final String s = new String(x);
        Executors.newSingleThreadExecutor().submit(() -> {
            VanillaAssetTree tree1 = new VanillaAssetTree("/").forRemoteAccess("localhost:8083", WIRE_TYPE);
            final ConcurrentMap<String, String> map1 = tree1.acquireMap(NAME, String.class,
                    String.class);
            for (; ; ) {
                for (int i = 0; i < 50; i++) {
                    map1.put("" + i, s);
                }
            }


        });

        YamlLogging.setAll(false);


        final ConcurrentMap<String, String> map;
        AssetTree tree3 = new VanillaAssetTree("/").forRemoteAccess("localhost:8083", WIRE_TYPE);

        map = tree3.acquireMap(NAME, String.class, String.class);


        tree3.registerSubscriber(NAME, MapEvent.class, o ->

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

