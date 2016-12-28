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
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.engine.Utils.yamlLoggger;

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
    private Map<ExceptionKey, Integer> exceptions;

    @Before
    public void before() throws IOException {
        exceptions = Jvm.recordExceptions();
        serverAssetTree1 = new VanillaAssetTree().forTesting();

        TCPRegistry.createServerSocketChannelFor(CONNECTION_1);

        serverEndpoint1 = new ServerEndpoint(CONNECTION_1, serverAssetTree1);

        client = new VanillaAssetTree("client1").forRemoteAccess
                (CONNECTION_1, WIRE_TYPE);
    }

    @After
    public void preAfter() {

        if (serverEndpoint1 != null)
            serverEndpoint1.close();

        client.close();
        serverAssetTree1.close();

        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }

    /**
     * simple test for bootstrap == FALSE
     */
    @Test(expected = java.lang.UnsupportedOperationException.class)
    public void testAcquireMapBootstrap() throws InterruptedException {
        @NotNull final Map<String, String> map1 = client.acquireMap(NAME, String.class, String.class);
        map1.put("pre-boostrap", "value");
        client.acquireMap(NAME + "?bootstrap=false", String.class, String.class);
    }

    /**
     * simple test for bootstrap == FALSE
     *
     * this test was written due to :
     *
     * CE-156 Disable bootstrapping on topic subscriptions doesn’t work in Java (?bootstrap=false).
     */
    @Test
    public void testTopicSubscriptionBootstrapFalse() throws InterruptedException {

        @NotNull final Map<String, String> map1 = client.acquireMap(NAME, String.class, String.class);
        map1.put("pre-bootstrap", "pre-bootstrap");

        @NotNull final BlockingQueue<String> q2 = new ArrayBlockingQueue<>(1);

        YamlLogging.setAll(false);

        yamlLoggger(() -> {
            client.registerTopicSubscriber(NAME + "?bootstrap=false",
                    String.class,
                    String.class,
                    (topic, message) -> {
                        q2.add(message);
                    });

            Jvm.pause(500);

            map1.put("post-bootstrap", "post-bootstrap");

            // wait for the post-bootstrap event
            try {
                Assert.assertEquals("post-bootstrap", q2.poll(20, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Jvm.rethrow(e);
            }

        });
    }
}
