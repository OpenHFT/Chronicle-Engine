package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Rob Austin
 */

public class MainClusterClient {
    public static final WireType WIRE_TYPE = WireType.COMPRESSED_BINARY;
    public static final int entries = 10;
    public static final String NAME = "/ChMaps/test?entries=" + entries + "&averageValueSize=" + (2 << 20);


    private static AssetTree tree;


    @BeforeClass
    public static void before() throws IOException {
        YamlLogging.clientWrites = true;
        YamlLogging.clientReads = true;

        //YamlLogging.showServerWrites = true;

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        //    TCPRegistry.createServerSocketChannelFor("host.port1", "host.port2");
        //   WireType writeType = WireType.BINARY;

        tree = new VanillaAssetTree("/").forRemoteAccess("localhost:8083", WIRE_TYPE);

    }

    @AfterClass
    public static void after() throws IOException {


        if (tree != null)
            tree.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        // TODO TCPRegistery.assertAllServersStopped();
    }


    @NotNull
    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

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

        tree = new VanillaAssetTree("/").forRemoteAccess("localhost:8083", WIRE_TYPE);

        YamlLogging.setAll(false);


        final ConcurrentMap<String, String> map;

        map = tree.acquireMap(NAME, String.class, String.class);


        tree.registerSubscriber(NAME, MapEvent.class, o -> System.out.println(o));


        for (; ; ) {
            try {
                Thread.sleep(5000);
            } catch (Exception ignore) {

            }
        }

    }


}

