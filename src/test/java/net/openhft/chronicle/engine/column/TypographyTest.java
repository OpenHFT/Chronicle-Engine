package net.openhft.chronicle.engine.column;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class TypographyTest {

    public static final String ADD_MAP_LATER = "/add/map/later";

    public static final String ADD_QUEUE_LATER = "/proc/connections/cluster/throughput/1";

    @NotNull
    @Rule
    public TestName name = new TestName();
    @NotNull
    String methodName = "";

    @NotNull
    private final VanillaAssetTree assetTree;
    @Nullable
    private final ServerEndpoint serverEndpoint;
    private MapView<String, String> map;


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Boolean[][]{
                {false}, {true}
        });
    }

    public TypographyTest(Boolean isRemote) throws Exception {

        if (isRemote) {
            @NotNull VanillaAssetTree assetTree0 = new VanillaAssetTree().forTesting();

            @NotNull String hostPortDescription = "SimpleQueueViewTest-methodName" + methodName;
            TCPRegistry.createServerSocketChannelFor(hostPortDescription);
            serverEndpoint = new ServerEndpoint(hostPortDescription, assetTree0);

            @NotNull final VanillaAssetTree client = new VanillaAssetTree();
            assetTree = client.forRemoteAccess(hostPortDescription, WireType.BINARY);

            //  assetTree.acquireMap(ADD_MAP_LATER, String.class, String.class).size();
            Executors.newScheduledThreadPool(1).schedule((Runnable)
                    () -> {

                        @NotNull final RequestContext requestContext = requestContext(ADD_QUEUE_LATER);

                        if (requestContext.bootstrap() != null)
                            throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                                    "acquiring a queue");

                        assetTree0.acquireView(requestContext.view("queue").type(String.class).type2(String.class)
                                .cluster(""));

                        assetTree0.acquireMap(ADD_MAP_LATER,
                                String.class, String.class).size();

                    }, 500, TimeUnit.MILLISECONDS);


        } else {
            assetTree = (new VanillaAssetTree(1)).forTesting();
            assetTree.acquireMap(ADD_MAP_LATER, String.class, String.class).size();

            @NotNull final RequestContext requestContext = requestContext(ADD_QUEUE_LATER);

            if (requestContext.bootstrap() != null)
                throw new UnsupportedOperationException("Its not possible to set the bootstrap when " +
                        "acquiring a queue");

            assetTree.acquireView(requestContext.view("queue").type(String.class).type2(String.class)
                    .cluster(""));
            serverEndpoint = null;
        }

    }


    @Before
    public void before() {
        assetTree.acquireMap("/example/data1", String.class, String.class).size();
        assetTree.acquireMap("/example/data2", String.class, String.class).size();
    }

    @Test
    public void testTypography() throws InterruptedException {

        //  map.put("hello", "world");
        //YamlLogging.setAll(true);
        @NotNull CountDownLatch latch = new CountDownLatch(1);
        @NotNull RequestContext rc = RequestContext.requestContext("").elementType(TopologicalEvent.class)
                .bootstrap(true);

        @NotNull Subscriber<TopologicalEvent> subscriber = o -> {
            if ("/example/data2".contentEquals(o.fullName()) && o.viewTypes()
                    .contains(MapView.class)) {
                latch.countDown();
            }
        };

        assetTree.acquireSubscription(rc).registerSubscriber(rc, subscriber, Filter.empty());

        latch.await(100000, TimeUnit.SECONDS);
    }

    @Test
    public void testWhenMapIsAddedLater() throws InterruptedException {

        //  map.put("hello", "world");
        YamlLogging.setAll(true);
        @NotNull CountDownLatch latch = new CountDownLatch(1);
        @NotNull RequestContext rc = RequestContext.requestContext("").elementType(TopologicalEvent.class)
                .bootstrap(true);

        @NotNull Subscriber<TopologicalEvent> subscriber = o -> {
            if (ADD_MAP_LATER.contentEquals(o.fullName()) && o.viewTypes()
                    .contains(MapView.class)) {
                latch.countDown();
            }
        };

        assetTree.acquireSubscription(rc).registerSubscriber(rc, subscriber, Filter.empty());

        latch.await(100000, TimeUnit.SECONDS);
    }


    @Test
    public void testWhenQueueIsAddedLater() throws InterruptedException {

        //  map.put("hello", "world");
        YamlLogging.setAll(true);
        @NotNull CountDownLatch latch = new CountDownLatch(1);
        @NotNull RequestContext rc = RequestContext.requestContext("").elementType(TopologicalEvent.class)
                .bootstrap(true);

        @NotNull Subscriber<TopologicalEvent> subscriber = o -> {
            if (ADD_QUEUE_LATER.contentEquals(o.fullName()) && o.viewTypes()
                    .contains(QueueView.class)) {
                latch.countDown();
            }
        };

        assetTree.acquireSubscription(rc).registerSubscriber(rc, subscriber, Filter.empty());

        latch.await(100000, TimeUnit.SECONDS);
    }


}
