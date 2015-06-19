/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.server.WireType;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static net.openhft.chronicle.engine.server.WireType.wire;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SubscriptionTestEvents extends ThreadMonitoringTest {
    private static int port;
    private static ConcurrentMap<String, String> map;
    private static final String NAME = "test";

    private static Boolean isRemote;

    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    @NotNull
    @Rule
    public TestName name = new TestName();
    private VanillaAssetTree serverAssetTree;

    @Before
    public void before() {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());

            ServerEndpoint serverEndpoint = null;
            try {
                serverEndpoint = new ServerEndpoint(serverAssetTree);
                port = serverEndpoint.getPort();
            } catch (IOException e) {
                Jvm.rethrow(e);
            }
            assetTree = new VanillaAssetTree().forRemoteAccess("localhost", port);
        } else
            assetTree = serverAssetTree;

        map = assetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void after() throws IOException {
        serverAssetTree.close();
        assetTree.close();
        if (map instanceof net.openhft.chronicle.core.io.Closeable)
            ((Closeable) map).close();
    }

    @Parameters
    public static Collection<Object[]> data() throws IOException {
        return Arrays.asList(
                new Object[]{Boolean.TRUE, WireType.TEXT}
                , new Object[]{Boolean.TRUE, WireType.BINARY}
                , new Object[]{Boolean.FALSE, WireType.TEXT}
                , new Object[]{Boolean.FALSE, WireType.BINARY}
        );
    }

    public SubscriptionTestEvents(Object isRemote, Object wireType) {
        this.isRemote = (Boolean) isRemote;

        wire = (Function<Bytes, Wire>) wireType;
    }

    @Test
    public void testSubscribeToChangesToTheMap() throws IOException, InterruptedException {

        final BlockingQueue<Object> eventsQueue = new ArrayBlockingQueue<Object>(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server";

                assetTree.registerSubscriber(NAME, MapEvent.class, eventsQueue::add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                map.put("Hello", "World");

                Object object = eventsQueue.take();
                Assert.assertTrue(object instanceof InsertedEvent);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    @Test
    public void testUnsubscribeToMapEvents() throws IOException, InterruptedException {

        final BlockingQueue eventsQueue = new ArrayBlockingQueue(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to key events. ";

                Subscriber<MapEvent> subscriber = eventsQueue::add;

                assetTree.registerSubscriber(NAME, MapEvent.class, subscriber);

                YamlLogging.writeMessage = "unsubscribes to changes to the map";
                assetTree.unregisterSubscriber(NAME, subscriber);

                YamlLogging.writeMessage = "puts the entry into the map, but since we have " +
                        "unsubscribed no event should be send form the server to the client";
                map.put("Hello", "World");

                Object object = eventsQueue.poll(1, SECONDS);
                Assert.assertNull(object);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    @Test
    public void testSubscribeToKeyEvents() throws IOException, InterruptedException {

        final BlockingQueue<Object> eventsQueue = new ArrayBlockingQueue<Object>(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server";

                assetTree.registerSubscriber(NAME, String.class, eventsQueue::add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                String expected = "Hello";
                map.put("Hello", "World");

                Object object = eventsQueue.take();
                Assert.assertTrue(object instanceof String);
                Assert.assertEquals(expected, object);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    @Test
    public void testUnSubscribeToKeyEvents() throws IOException, InterruptedException {

        final BlockingQueue<Object> eventsQueue = new ArrayBlockingQueue<Object>(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to key events. Then " +
                        "unsubscribes and puts and entry into the map, no subsequent event should" +
                        " be received from the server";

                assetTree.registerSubscriber(NAME, String.class, eventsQueue::add);
                assetTree.unregisterSubscriber(NAME, eventsQueue::add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                String expected = "World";
                map.put("Hello", expected);

                Object object = eventsQueue.take();
                Assert.assertNull(object);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    @Test
    public void testSubscribeToKeyEventsAndRemoveKey() throws IOException, InterruptedException {

        final BlockingQueue<Object> eventsQueue = new ArrayBlockingQueue<Object>(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map followed by a remove, notice " +
                        "that the " +
                        "'reply: Hello' is received twice, one for the put and one for the " +
                        "remove.";

                assetTree.registerSubscriber(NAME, String.class, eventsQueue::add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                String expected = "World";
                map.put("Hello", expected);
                map.remove("Hello");

                Object putEvent = eventsQueue.take();
                Object removeEvent = eventsQueue.take();
                Assert.assertTrue(putEvent instanceof String);
                Assert.assertTrue(removeEvent instanceof String);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    @Test
    public void testSubscribeToMapEventsAndRemoveKey() throws IOException, InterruptedException {

        final BlockingQueue<Object> eventsQueue = new ArrayBlockingQueue<Object>(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map followed by a remove, notice " +
                        "that the " +
                        "'reply: Hello' is received twice, one for the put and one for the " +
                        "remove.";

                assetTree.registerSubscriber(NAME, MapEvent.class, eventsQueue::add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                String expected = "World";
                map.put("Hello", expected);

                Thread.sleep(1);
                map.remove("Hello");

                Object putEvent = eventsQueue.take();
                Object removeEvent = eventsQueue.take();
                Assert.assertTrue(putEvent instanceof InsertedEvent);
                Assert.assertTrue(removeEvent instanceof RemovedEvent);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }

        });

    }

}



