/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
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
    private final boolean isRemote;
    private final WireType wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();

    public SubscriptionTest(boolean isRemote, WireType wireType) {
        this.isRemote = isRemote;
        this.wireType = wireType;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, null}
                , new Object[]{true, WireType.TEXT}
                , new Object[]{true, WireType.BINARY}
        );
    }

    public void preAfter() {
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Before
    public void before() {
        methodName(name.getMethodName());

    }

    @Test
    public void testSubscriptionTest() throws IOException, InterruptedException {
        MapEventListener<String, Factor> listener;

        @NotNull Factor factorXYZ = new Factor();
        factorXYZ.setAccountNumber("xyz");

        @NotNull Factor factorABC = new Factor();
        factorABC.setAccountNumber("abc");

        @NotNull Factor factorDDD = new Factor();
        factorDDD.setAccountNumber("ddd");

        listener = createMock(MapEventListener.class);
        listener.insert(NAME, "testA", factorXYZ);
        listener.insert(NAME, "testB", factorABC);
        listener.update(NAME, "testA", factorXYZ, factorDDD);
        listener.remove(NAME, "testA", factorDDD);
        listener.remove(NAME, "testB", factorABC);

        replay(listener);

        @NotNull VanillaAssetTree serverAssetTree = new VanillaAssetTree().forTesting();
        @Nullable ServerEndpoint serverEndpoint = null;
        @NotNull Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(listener);
        VanillaAssetTree assetTree;
        if (isRemote) {
            TCPRegistry.createServerSocketChannelFor("testSubscriptionTest.host.port");
            serverEndpoint = new ServerEndpoint("testSubscriptionTest.host.port", serverAssetTree);

            assetTree = new VanillaAssetTree().forRemoteAccess("testSubscriptionTest.host.port", wireType);
        } else {
            assetTree = serverAssetTree;
        }
        @NotNull ConcurrentMap<String, Factor> map = assetTree.acquireMap(NAME, String.class, Factor.class);

        yamlLoggger(() -> {
            System.out.print(":\n");
            YamlLogging.writeMessage("this is how to create a subscription");
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
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }
}

