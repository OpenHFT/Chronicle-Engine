package net.openhft.chronicle.engine;


import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RestartClosedPublisherTest {
    public static final WireType WIRE_TYPE = WireType.TEXT;
    private static final String CONNECTION_1 = "Test1.host.port";
    private ServerEndpoint _serverEndpoint1;
    private VanillaAssetTree _server;
    private VanillaAssetTree _remote;
    private String _testMapUri = "/test/map";

    @Before
    public void setUp() throws Exception {
        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);
        _server = new VanillaAssetTree().forServer();

        _serverEndpoint1 = new ServerEndpoint(CONNECTION_1, _server, WIRE_TYPE);

        createRemoteClient();
    }

    private void createRemoteClient() {
        _remote = new VanillaAssetTree().forRemoteAccess(CONNECTION_1, WIRE_TYPE);
    }

    /**
     * Test that a client can connect to a server side map, register a subscriber and perform put.
     * Close the client, create new one and do the same.
     */
    @Test
    public void testClientReconnectionOnMap() throws InterruptedException {
        String testKey = "Key1";
        String value = "Value1";

        BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(1);

        connectClientAndPerformPutGetTest(testKey, value, eventQueue);

        _remote.close();
        Jvm.pause(200);

        value = "Value2";
        connectClientAndPerformPutGetTest(testKey, value, eventQueue);
    }


    private void connectClientAndPerformPutGetTest(String testKey, String value, BlockingQueue<String> eventQueue) throws InterruptedException {
        _remote = new VanillaAssetTree().forRemoteAccess(CONNECTION_1, WIRE_TYPE);

        String keySubUri = _testMapUri + "/" + testKey + "?bootstrap=false";
        Map<String, String> map = _remote.acquireMap(_testMapUri, String.class, String.class);
        map.size();
        _remote.registerSubscriber(keySubUri + "?bootstrap=false", String.class, (e) -> eventQueue.add(e));

        // wait for the subscription to be read by the server
        Thread.sleep(100);

        map.put(testKey, value);
        Assert.assertEquals(value, eventQueue.poll(20, SECONDS));
        String getValue = map.get(testKey);
        Assert.assertEquals(value, getValue);
    }
}