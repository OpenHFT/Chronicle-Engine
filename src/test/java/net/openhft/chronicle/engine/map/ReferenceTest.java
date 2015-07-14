package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.pubsub.SimpleSubscription;
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

        YamlLogging.clientReads=true;
        YamlLogging.clientWrites=true;
        YamlLogging.showServerReads=true;
        YamlLogging.showServerWrites=true;
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
        TCPRegistry.assertAllServersStopped();
    }

    @Test
    public void testRemoteReference() throws IOException {
        Map map = assetTree.acquireMap("group", String.class, String.class);

        map.put("subject", "cs");
        assertEquals("cs", map.get("subject"));

        Reference<String> ref = assetTree.acquireReference("group/subject", String.class);
        ref.set("sport");
        assertEquals("sport", map.get("subject"));
        assertEquals("sport", ref.get());

        ref.getAndSet("biology");
        assertEquals("biology", ref.get());

        String s = ref.getAndRemove();
        assertEquals("biology", s);

        ref.set("physics");
        assertEquals("physics", ref.get());

        ref.remove();
        assertEquals(null, ref.get());

        ref.set("chemistry");
        assertEquals("chemistry", ref.get());

        s = ref.applyTo(o -> "applied_" + o.toString());
        assertEquals("applied_chemistry", s);

        ref.asyncUpdate(o -> "**" + o.toString());
        assertEquals("**chemistry", ref.get());

        ref.set("maths");
        assertEquals("maths", ref.get());

        s = ref.syncUpdate(o -> "**" + o.toString(), o -> "**" + o.toString());
        assertEquals("****maths", s);
        assertEquals("**maths", ref.get());
    }

    @Test
    public void testReferenceSubscriptions(){
        Map map = assetTree.acquireMap("group", String.class, String.class);

        map.put("subject", "cs");
        assertEquals("cs", map.get("subject"));

        Reference<String> ref = assetTree.acquireReference("group/subject", String.class);
        ref.set("sport");
        assertEquals("sport", map.get("subject"));
        assertEquals("sport", ref.get());

        Subscriber<String> subscriber = s -> System.out.println("*****Message Received " + s);
        assetTree.registerSubscriber("group/subject", String.class, subscriber);

        ref.set("maths");

        Jvm.pause(200);
    }
}
