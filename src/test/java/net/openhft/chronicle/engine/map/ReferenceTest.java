package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 13/07/2015.
 */
@RunWith(value = Parameterized.class)
public class ReferenceTest {
    private static Boolean isRemote;
    private static final String NAME = "test";
    @Rule
    public TestName name = new TestName();
    WireType WIRE_TYPE = WireType.TEXT;
    VanillaAssetTree serverAssetTree;
    AssetTree assetTree;
    private ServerEndpoint serverEndpoint;


    public ReferenceTest(Object isRemote, Object wireType) {
        ReferenceTest.isRemote = (Boolean) isRemote;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {
        return Arrays.asList(
                new Object[]{Boolean.FALSE, WireType.TEXT}
            //    , new Object[]{Boolean.FALSE, WireType.BINARY}
                , new Object[]{Boolean.TRUE, WireType.TEXT}
            //    , new Object[]{Boolean.TRUE, WireType.BINARY}
        );
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());
            TCPRegistry.createServerSocketChannelFor("SubscriptionEventTest.host.port");
            serverEndpoint = new ServerEndpoint("SubscriptionEventTest.host.port", serverAssetTree, WIRE_TYPE);

            assetTree = new VanillaAssetTree().forRemoteAccess("SubscriptionEventTest.host.port", WIRE_TYPE);
        } else
            assetTree = serverAssetTree;

    }

    @After
    public void after() throws IOException {
        assetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        //if (map instanceof Closeable)
        //    ((Closeable) map).close();
        TCPRegistry.assertAllServersStopped();
    }

    @Test
    public void testRemoteReference() throws IOException {

        YamlLogging.clientReads=true;
        YamlLogging.clientWrites=true;
        YamlLogging.showServerReads=true;
        YamlLogging.showServerWrites=true;

        Map map = assetTree.acquireMap("group", String.class, String.class);
        map.put("topic", "cs");

        System.out.println(map.get("topic"));
        Reference ref = assetTree.acquireReference("group/topic", String.class);

        //test the set method
        ref.set("sport");

        assertEquals("sport", map.get("topic"));

        assertEquals("sport", ref.get());

        //ref.applyTo(o -> "applied");
    }
}
