package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class MapBootstrapTest extends ThreadMonitoringTest {


    public static final WireType WIRE_TYPE = WireType.TEXT;
    private static final String NAME = "MapBootstrapTest";
    private static final String CONNECTION_1 = "MapBootstrapTest.host.port";

    private AssetTree client;

    private VanillaAssetTree serverAssetTree1;
    private ServerEndpoint serverEndpoint1;

    @Before
    public void before() throws IOException {
        serverAssetTree1 = new VanillaAssetTree().forTesting();

        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);

        serverEndpoint1 = new ServerEndpoint(CONNECTION_1, serverAssetTree1, WIRE_TYPE);

        client = new VanillaAssetTree("client1").forRemoteAccess
                (CONNECTION_1, WIRE_TYPE);
    }

    @After
    public void after() throws IOException {

        if (serverEndpoint1 != null)
            serverEndpoint1.close();

        client.close();
        serverAssetTree1.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }


    /**
     * simple test for bootstrap == FALSE
     *
     * this test was written due to :
     *
     * CE-156 Disable bootstrapping on topic subscriptions doesn’t work in Java (?bootstrap=false).
     */
    @Test
    public void testSimpleMapBootstrapFalse() throws InterruptedException {

        final Map<String, String> map1 = client.acquireMap(NAME, String.class, String.class);
        map1.put("pre-boostrap", "value");

        final MapView<String, String> clientMap = client.acquireMap(
                NAME + "?bootstrap=false", String.class, String.class);

        final BlockingQueue q2 = new ArrayBlockingQueue(1);
        Assert.assertEquals(null, clientMap.get("pre-boostrap"));

        client.registerSubscriber(NAME + "?bootstrap=false", MapEvent.class, q2::add);
        map1.put("post-boostrap", "value");

        // wait for an event
        final Object poll = q2.poll(20, TimeUnit.SECONDS);
        Assert.assertTrue(poll instanceof InsertedEvent);
        Assert.assertEquals("post-boostrap", ((InsertedEvent<String, String>) poll).getKey());

        Assert.assertEquals("value", clientMap.get("post-boostrap"));
    }
}
