package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class Test2 {


    private static final String CONNECTION = "host.port.KeySubscriptionTest";
    public static final WireType WIRE_TYPE = WireType.TEXT;
    private VanillaAssetTree serverAssetTree;

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        TCPRegistry.createServerSocketChannelFor(CONNECTION);
        new ServerEndpoint(CONNECTION, serverAssetTree, WIRE_TYPE);

    }


    /**
     * run this with a profiler
     *
     * @throws InterruptedException
     */
    @Test
    public void test2() throws InterruptedException {

        VanillaAssetTree clientTree = new VanillaAssetTree().forRemoteAccess(CONNECTION, WIRE_TYPE);
        final TcpChannelHub hub = clientTree.root().findView(TcpChannelHub.class);

        assert hub != null;

        for (; ; ) {

            Thread.sleep(2000);    // give time for the disconnect and reconnect
            hub.forceDisconnect();

        }

    }

}
