package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static java.util.stream.Collectors.averagingInt;
import static net.openhft.chronicle.engine.Utils.methodName;

/**
 * @author Rob Austin.
 */


/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class QueryableKeySetTest extends ThreadMonitoringTest {

    private static final String NAME = "test";
    public String connection = "QueryableTest.host.port";
    private static MapView<String, String> map;
    private final Boolean isRemote;
    private final WireType wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public QueryableKeySetTest(Object isRemote, WireType wireType) {
        this.isRemote = (Boolean) isRemote;
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        final List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{Boolean.FALSE, WireType.BINARY});
        list.add(new Object[]{Boolean.TRUE, WireType.BINARY});
        list.add(new Object[]{Boolean.FALSE, WireType.TEXT});
        list.add(new Object[]{Boolean.TRUE, WireType.TEXT});

        return list;
    }


    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());

            YamlLogging.showServerWrites = true;
            YamlLogging.showServerReads = true;

            connection = "StreamTest." + name.getMethodName() + ".host.port";
            TCPRegistry.createServerSocketChannelFor(connection);
            serverEndpoint = new ServerEndpoint(connection, serverAssetTree, wireType);
            assetTree = new VanillaAssetTree().forRemoteAccess(connection, wireType);
        } else
            assetTree = serverAssetTree;

        map = assetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void after() throws IOException {
        assetTree.close();
        Jvm.pause(1000);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        if (map instanceof Closeable)
            ((Closeable) map).close();
        TCPRegistry.reset();
    }

    @Test
    public void testQueryForEach() throws Exception {

        final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");

        final Query<String> query = map.keySet().query();
        final Set<String> result = new HashSet<>();
        query.forEach(result::add);

        Assert.assertEquals(new HashSet<>(Arrays.asList("1", "2")), result);
    }


    @Test
    public void testQueryForEachWithPredicate() throws Exception {

        final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");

        YamlLogging.showServerReads = true;
        YamlLogging.showServerWrites = true;

        final KeySetView<String> remoteSetView = map.keySet();
        final Query<String> query = remoteSetView.query();

        final Set<String> result = new HashSet<>();
        query.filter("1"::equals).forEach(result::add);
        Assert.assertEquals(new HashSet<>(Arrays.asList("1")), result);
    }


    @Test
    public void testQueryForCollect() throws Exception {

        final MapView<Integer, Integer> map = assetTree.acquireMap("name", Integer.class, Integer
                .class);

        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        final Query<Integer> query = map.keySet().query();
        Double x = query.filter((obj) -> obj >= 1 && obj <= 2).collect(averagingInt(v -> (int) v));
        Assert.assertEquals((Double) 1.5, x);
    }

    @Test
    public void testForEach() throws Exception {

        final MapView<Integer, Integer> map = assetTree.acquireMap("name", Integer.class, Integer
                .class);

        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        final Query<Integer> query = map.keySet().query();
        query.filter((obj) -> obj >= 1 && obj <= 2).forEach(System.out::println);

    }


}

