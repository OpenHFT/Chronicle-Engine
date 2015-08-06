package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.Test;

/**
 * @author Rob Austin.
 */
public class SteamTest {

    public static final String CONNECTION = "SteamTest.host.port";

    @Test
    public void testStreamsLocal() throws Exception {

        final VanillaAssetTree assetTree = new VanillaAssetTree().forTesting();

        final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");
        System.out.println(map.keySet().stream().count());
    }

    @Test
    public void testStreamsRemote() throws Exception {

        final VanillaAssetTree assetTree = new VanillaAssetTree().forTesting();
        TCPRegistry.createServerSocketChannelFor(CONNECTION);
        final ServerEndpoint serverEndpoint = new ServerEndpoint(CONNECTION, assetTree, WireType.TEXT);

        VanillaAssetTree client = new VanillaAssetTree("client1").forRemoteAccess
                (CONNECTION, WireType.TEXT);


        final MapView<String, String> map = assetTree.acquireMap("name", String.class, String
                .class);

        map.put("1", "1");
        map.put("2", "2");
        System.out.println(map.keySet().stream().count());

        client.close();
        assetTree.close();
        serverEndpoint.close();
        TCPRegistry.reset();

    }

}

