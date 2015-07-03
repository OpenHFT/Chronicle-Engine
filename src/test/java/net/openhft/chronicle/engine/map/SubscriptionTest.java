/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.Factor;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistery;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SubscriptionTest extends ThreadMonitoringTest {
    private static final String NAME = "/test";
    private static ConcurrentMap<String, Factor> map;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private final boolean isRemote;

    public SubscriptionTest(Boolean isRemote) {
        this.isRemote = isRemote;
    }

    @Parameters
    public static Collection<Object[]> data() throws IOException {

        return Arrays.asList(new Boolean[][]{
                {false},
                {true}
        });
    }

    @Before
    public void before() {
        methodName(name.getMethodName());
    }

    @AfterClass
    public static void tearDownClass() {
        TCPRegistery.assertAllServersStopped();
    }

    @Test
    public void testSubscriptionTest() throws IOException, InterruptedException {
        MapEventListener<String, Factor> listener;

        Factor factorXYZ = new Factor();
        factorXYZ.setAccountNumber("xyz");

        Factor factorABC = new Factor();
        factorABC.setAccountNumber("abc");

        Factor factorDDD = new Factor();
        factorDDD.setAccountNumber("ddd");

        listener = createMock(MapEventListener.class);
        listener.insert(NAME, "testA", factorXYZ);
        listener.insert(NAME, "testB", factorABC);
        listener.update(NAME, "testA", factorXYZ, factorDDD);
        listener.remove(NAME, "testA", factorDDD);
        listener.remove(NAME, "testB", factorABC);

        replay(listener);

        VanillaAssetTree serverAssetTree = new VanillaAssetTree().forTesting();
        ServerEndpoint serverEndpoint = null;
        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(listener);
        VanillaAssetTree assetTree;
        if (isRemote) {
            WireType wireType = WireType.TEXT;
            TCPRegistery.createServerSocketChannelFor("testSubscriptionTest.host.port");
            serverEndpoint = new ServerEndpoint("testSubscriptionTest.host.port", serverAssetTree, wireType);

            assetTree = new VanillaAssetTree().forRemoteAccess("testSubscriptionTest.host.port", wireType);
        } else {
            assetTree = serverAssetTree;
        }
        ConcurrentMap<String, Factor> map = assetTree.acquireMap(NAME, String.class, Factor.class);

        yamlLoggger(() -> {
            System.out.print(":\n");
            YamlLogging.writeMessage = "this is how to create a subscription";
            assetTree.registerSubscriber(NAME, MapEvent.class, mapEventSubscriber);
        });


        yamlLoggger(() -> {
            //test an insert
            map.put("testA", factorXYZ);
            assertEquals(1, map.size());
            assertEquals("xyz", map.get("testA").getAccountNumber());

            //test another insert
            map.put("testB", factorABC);
            assertEquals("abc", map.get("testB").getAccountNumber());

            //Test an update
            map.put("testA", factorDDD);
            assertEquals("ddd", map.get("testA").getAccountNumber());

            //Test a remove
            map.remove("testA");
            map.remove("testB");

            Jvm.pause(100);

            assetTree.unregisterSubscriber(NAME, mapEventSubscriber);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //Test that after unregister we don't get events
            map.put("testC", factorXYZ);
        });

        assetTree.close();
        if (serverEndpoint != null) serverEndpoint.close();
        serverAssetTree.close();


        verify(listener);
    }


}

