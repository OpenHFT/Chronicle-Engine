/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.engine.Utils.methodName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by daniel on 13/07/2015.
 */
@RunWith(value = Parameterized.class)
public class ReferenceTest {
    private static AtomicReference<Throwable> t = new AtomicReference<>();
    @NotNull
    @Rule
    public TestName name = new TestName();
    @NotNull
    WireType wireType;
    VanillaAssetTree serverAssetTree;
    AssetTree assetTree;
    private boolean isRemote;
    private ServerEndpoint serverEndpoint;
    private String hostPortToken;

    public ReferenceTest(boolean isRemote, WireType wireType) {
        this.wireType = wireType;
        this.isRemote = isRemote;
        YamlLogging.setAll(false);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                 new Object[]{false, null}
                , new Object[]{true, WireType.TEXT}
//                , new Object[]{true, WireType.BINARY}
        );
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Before
    public void before() throws IOException {
        hostPortToken = "ReferenceTest.host.port";
        serverAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));

        if (isRemote) {

            methodName(name.getMethodName());
            TCPRegistry.createServerSocketChannelFor(hostPortToken);
            serverEndpoint = new ServerEndpoint(hostPortToken, serverAssetTree);

            assetTree = new VanillaAssetTree().forRemoteAccess(hostPortToken, wireType, x -> t.set
                    (x));
        } else {
            assetTree = serverAssetTree;
        }
    }

    @After
    public void after() throws IOException {
        assetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        //TCPRegistry.assertAllServersStopped();

        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
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
    public void testReferenceSubscriptions() throws InterruptedException {
        Map map = assetTree.acquireMap("group", String.class, String.class);

        map.put("subject", "cs");
        assertEquals("cs", map.get("subject"));

        Reference<String> ref = assetTree.acquireReference("group/subject", String.class);
        ref.set("sport");
        assertEquals("sport", map.get("subject"));
        assertEquals("sport", ref.get());
        CountDownLatch lacth1 = new CountDownLatch(1);

        CountDownLatch lacth2 = new CountDownLatch(2);
        CountDownLatch lacth3 = new CountDownLatch(3);
        List<String> events = new ArrayList<>();
        Subscriber<String> subscriber = s -> {
            events.add(s);
            lacth1.countDown();
            lacth2.countDown();
            lacth3.countDown();
        };

        assetTree.registerSubscriber("group/subject?bootstrap=true", String.class, subscriber);
        lacth1.await(20, TimeUnit.SECONDS);
        assertEquals("sport", events.get(0));//bootstrap

        ref.set("maths");
        lacth2.await(20, TimeUnit.SECONDS);
        assertEquals("maths", events.get(1));

        ref.set("cs");
        lacth3.await(20, TimeUnit.SECONDS);
        assertEquals("cs", events.get(2));
    }

    @Test
    public void testAssetReferenceSubscriptions() {
        Map map = assetTree.acquireMap("group", String.class, String.class);
        //TODO The child has to be in the map before you register to it
        map.put("subject", "init");

        List<String> events = new ArrayList<>();

        Subscriber<String> keyEventSubscriber = new Subscriber<String>() {
            @Override
            public void onMessage(String s) {
                events.add(s);
            }

            @Override
            public void onEndOfSubscription() {
                events.add("END");
            }
        };

        assetTree.registerSubscriber("group" + "/" + "subject" + "?bootstrap=false&putReturnsNull=true", String.class, keyEventSubscriber);

        // Jvm.pause(100);
        Asset child = assetTree.getAsset("group").getChild("subject");
        assertNotNull(child);
        SubscriptionCollection subscription = child.subscription(false);

        while (subscription.subscriberCount() == 0) {

        }

        assertEquals(1, subscription.subscriberCount());

        assetTree.unregisterSubscriber("group" + "/" + "subject", keyEventSubscriber);

        while (subscription.subscriberCount() != 0) {

        }
        assertEquals(0, subscription.subscriberCount());

    }

    @Test
    public void testAssetReferenceSubscriptionsBootstrapTrue() {
        Map map = assetTree.acquireMap("group", String.class, String.class);
        //TODO The child has to be in the map before you register to it
        map.put("subject", "init");

        List<String> events = new ArrayList<>();

        Subscriber<String> keyEventSubscriber = new Subscriber<String>() {
            @Override
            public void onMessage(String s) {
                events.add(s);
            }

            @Override
            public void onEndOfSubscription() {
                events.add("END");
            }
        };

        assetTree.registerSubscriber("group" + "/" + "subject" + "?bootstrap=true&putReturnsNull=true", String.class, keyEventSubscriber);

        Jvm.pause(100);
        Asset child = assetTree.getAsset("group").getChild("subject");
        assertNotNull(child);
        SubscriptionCollection subscription = child.subscription(false);

        assertEquals(1, subscription.subscriberCount());

        map.put("subject", "cs");
        map.put("subject", "maths");

        assetTree.unregisterSubscriber("group" + "/" + "subject", keyEventSubscriber);

        Jvm.pause(100);
        assertEquals(0, subscription.subscriberCount());

        assertEquals("init", events.get(0));
        assertEquals("cs", events.get(1));
        assertEquals("maths", events.get(2));
        assertEquals("END", events.get(3));
    }

    @Test
    public void testSubscriptionMUFG() {
        String key = "subject";
        String _mapName = "group";
        Map map = assetTree.acquireMap(_mapName, String.class, String.class);
        //TODO does not work without an initial put
        map.put(key, "init");

        List<String> events = new ArrayList<>();
        Subscriber<String> keyEventSubscriber = s -> {
            System.out.println("** rec:" + s);
            events.add(s);
        };

        assetTree.registerSubscriber(_mapName + "/" + key + "?bootstrap=false&putReturnsNull=true", String.class, keyEventSubscriber);
        // TODO CHENT-49
        Jvm.pause(100);
        Asset child = assetTree.getAsset(_mapName).getChild(key);
        assertNotNull(child);
        SubscriptionCollection subscription = child.subscription(false);
        assertEquals(1, subscription.subscriberCount());

        YamlLogging.showServerWrites(true);
        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average

        AtomicInteger count = new AtomicInteger();
        // IntStream.range(0, 3).forEach(i ->
        // {
        //_testMap.put(key, _twoMbTestString + i);
        map.put(key, "" + count.incrementAndGet());
        map.put(key, "" + count.incrementAndGet());
        map.put(key, "" + count.incrementAndGet());
        // });

        for (int i = 0; i < 100; i++) {
            if (events.size() == 3)
                break;
            Jvm.pause(150);
        }

        assertEquals(3, events.size());
        assetTree.unregisterSubscriber(_mapName + "/" + key, keyEventSubscriber);

        Jvm.pause(100);
        assertEquals(0, subscription.subscriberCount());
    }
}
