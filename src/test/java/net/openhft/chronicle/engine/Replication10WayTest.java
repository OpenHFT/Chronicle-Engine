package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.fs.ChronicleMapGroupFS;
import net.openhft.chronicle.engine.fs.Cluster;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.FilePerKeyGroupFS;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Created by Rob Austin
 */
@RunWith(Parameterized.class)
public class Replication10WayTest {

    public static final int MAX = 10;
    public static final String CLUSTER_NAME = "max-cluster";
    public static final WireType WIRE_TYPE = WireType.TEXT;
    public static final String NAME = "/ChMaps/test";
    private static AtomicReference<Throwable> t = new AtomicReference<>();
    public ServerEndpoint[] serverEndpoints = new ServerEndpoint[MAX];
    private AssetTree trees[] = new AssetTree[MAX];

    public Replication10WayTest() {
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[3][0]);
    }

    @NotNull
    private static AssetTree create(final int hostId, Function<Bytes, Wire> writeType) {
        AssetTree tree = new VanillaAssetTree((byte) hostId)
                .forTesting(x -> t.compareAndSet(null, x))
                .withConfig(resourcesDir() + "/cmkvst", OS.TARGET + "/" + hostId);

        tree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                VanillaMapView::new,
                KeyValueStore.class);
        tree.root().addLeafRule(EngineReplication.class, "Engine replication holder",
                CMap2EngineReplicator::new);
        tree.root().addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).cluster(CLUSTER_NAME),
                        asset));

        addHostDenialsToCluster(tree, MAX);

        return tree;
    }

    private static void addHostDenialsToCluster(AssetTree tree, final int numberOfHosts) {
        Clusters clusters = tree.root().findView(Clusters.class);
        final Cluster localCluster = new Cluster(CLUSTER_NAME);

        final TextWire t = new TextWire(Bytes.elasticByteBuffer());
        for (int i = 0; i < numberOfHosts; i++) {
            final int j = i;

            t.writeEventName(() -> ("host" + j)).marshallable(m -> {
                m.writeEventName(() -> "hostId").int32(j);
                m.writeEventName(() -> "tcpBufferSize").int32(65536);
                m.writeEventName(() -> "connectUri").text("host.port" + j);
                m.writeEventName(() -> "timeoutMs").int32(1000);
            });

        }
        localCluster.readMarshallable(t);

        clusters.put(CLUSTER_NAME, localCluster);
    }

    @NotNull
    public static String resourcesDir() {
        String path = ChronicleMapKeyValueStoreTest.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        if (path == null)
            return ".";
        return new File(path).getParentFile().getParentFile() + "/src/test/resources";
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null)
            Jvm.rethrow(th);

    }

    @Before
    public void before() throws IOException {
        YamlLogging.setAll(false);

        ClassAliasPool.CLASS_ALIASES.addAlias(ChronicleMapGroupFS.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(FilePerKeyGroupFS.class);
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TARGET, NAME));

        WireType writeType = WireType.TEXT;

        for (int i = 1; i < MAX; i++) {
            final String hostPort = "host.port" + i;
            TCPRegistry.createServerSocketChannelFor(hostPort);
            trees[i] = create(i, writeType);
            assert trees[i] != null;

            serverEndpoints[i] = new ServerEndpoint(hostPort, trees[i], writeType);

        }
    }

    @After
    public void after() throws IOException {

        shutdownTrees();
        Jvm.pause(100);
        shutDownServers();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        // TODO TCPRegistery.assertAllServersStopped();
    }

    private void shutdownTrees() {
        ArrayList<Future> futures = new ArrayList<>();

        ExecutorService c = Executors.newCachedThreadPool(new NamedThreadFactory("Tree Closer",
                true));
        for (int i = 1; i < MAX; i++) {
            final int j = i;
            futures.add(c.submit(trees[j]::close));

        }

        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void shutDownServers() {
        ArrayList<Future> futures = new ArrayList<>();

        ExecutorService c = Executors.newCachedThreadPool(new NamedThreadFactory("Servers Closer", true));
        for (int i = 1; i < MAX; i++) {
            final int j = i;
            futures.add(c.submit(serverEndpoints[j]::close));
        }

        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void test() throws InterruptedException {

//      YamlLogging.showServerWrites = true;
//      YamlLogging.showServerReads = true;
        ConcurrentMap[] maps = new ConcurrentMap[MAX];

        for (int i = 1; i < MAX; i++) {
            maps[i] = trees[i].acquireMap(NAME, String.class, String.class);
            assertNotNull(maps[i]);
        }

        for (int i = 1; i < MAX; i++) {
            maps[i].put("hello" + i, "world" + i);
        }

        OUTER:
        for (int i = 1; i <= 100; i++) {

            for (int j = 1; i < MAX; i++) {
                if (maps[j].size() != MAX - 1) {
                    Jvm.pause(1000);
                    continue OUTER;
                }
            }

            break;

        }

        for (int i = 1; i < MAX; i++) {
            Assert.assertEquals("world" + i, maps[i].get("hello" + i));
        }
    }

}

