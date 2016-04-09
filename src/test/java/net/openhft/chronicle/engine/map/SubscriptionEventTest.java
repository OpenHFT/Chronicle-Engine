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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.WireType;
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
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SubscriptionEventTest extends ThreadMonitoringTest {
    private static final String NAME = "test";

    private static AtomicReference<Throwable> t = new AtomicReference();
    private static MapView<String, String> map;
    private final Boolean isRemote;
    private final WireType wireType;
    @NotNull
    @Rule
    public TestName name = new TestName();
    private AssetTree assetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    public SubscriptionEventTest(boolean isRemote, WireType wireType) {
        this.isRemote = isRemote;
        this.wireType = wireType;
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, null}
                // check connection is fine after a reconnect CE-187
                , new Object[]{true, WireType.TEXT}
                , new Object[]{true, WireType.BINARY}
        );
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));

        if (isRemote) {

            methodName(name.getMethodName());
            final String hostPort = "SubscriptionEventTest." + name.getMethodName() + ".host.port";
            TCPRegistry.createServerSocketChannelFor(hostPort);
            serverEndpoint = new ServerEndpoint(hostPort, serverAssetTree);

            assetTree = new VanillaAssetTree().forRemoteAccess(hostPort, wireType, x -> t.set(x));
        } else {
            assetTree = serverAssetTree;
        }

        map = assetTree.acquireMap(NAME, String.class, String.class);
        YamlLogging.setAll(false);
    }

    @After
    public void after() throws IOException {
        assetTree.close();
        Jvm.pause(100);
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        if (map instanceof Closeable)
            ((Closeable) map).close();
        //   TCPRegistry.assertAllServersStopped();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();
    }

    @Test
    public void testSubscribeToChangesToTheMap() throws IOException, InterruptedException {

        final BlockingQueue<MapEvent> eventsQueue = new LinkedBlockingQueue<>();

        YamlLogging.showServerWrites(true);
        YamlLogging.showServerReads(true);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage("Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server");

                Subscriber<MapEvent> add = eventsQueue::add;
                assetTree.registerSubscriber(NAME, MapEvent.class, add);

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");
                map.put("Hello", "World");
                assertEquals(1, map.size());

                MapEvent object = eventsQueue.poll(2, SECONDS);
                Assert.assertTrue("Object was " + object, object instanceof InsertedEvent);

                assetTree.unregisterSubscriber(NAME, add);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    /**
     * REMOTE ONLY TEST
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testPushingEntriesToTheServerDirectly() throws IOException, InterruptedException {

        if (!isRemote)
            return;

        final BlockingQueue<MapEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {

                Subscriber<MapEvent> add = eventsQueue::add;
                assetTree.registerSubscriber(NAME, MapEvent.class, add);

                final Map serverMap = serverAssetTree.acquireMap(NAME, String.class, String.class);

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");

                serverMap.put("sever-key", "server-value");
                map.put("hello", "world");

                Object object = eventsQueue.take();
                eventsQueue.take();
                Assert.assertTrue(object instanceof InsertedEvent);

                // server map
                Assert.assertEquals(2, serverMap.size());

                // client map
                Assert.assertEquals(2, map.size());

                assetTree.unregisterSubscriber(NAME, add);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Test
    public void testTopicSubscribe() throws InvalidSubscriberException {

        YamlLogging.setAll(false);

        class TopicDetails<T, M> {
            private final M message;
            private final T topic;

            public TopicDetails(final T topic, final M message) {
                this.topic = topic;
                this.message = message;
            }

            @NotNull
            @Override
            public String toString() {
                return "TopicDetails{" +
                        "message=" + message +
                        ", topic=" + topic +
                        '}';
            }
        }

        final BlockingQueue<TopicDetails> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                // todo fix the text
                YamlLogging.writeMessage("Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server");

                TopicPublisher<String, String> topicPublisher = assetTree.acquireTopicPublisher(NAME, String.class, String.class);

                TopicSubscriber<String, String> subscriber = (topic, message) -> eventsQueue.add(new TopicDetails<>(topic, message));
                assetTree.registerTopicSubscriber(NAME, String.class, String.class,
                        subscriber);

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");
                topicPublisher.publish("Hello", "World");

                TopicDetails take = eventsQueue.take();
                System.out.println(take);

                // todo fix the text for the unsubscribe.
                assetTree.unregisterTopicSubscriber(NAME, subscriber);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });

    }

    @Test
    public void testUnsubscribeToMapEvents() throws IOException, InterruptedException {

        // not supported for remote
        if (isRemote)
            return;

        final BlockingQueue<MapEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage("Sets up a subscription to listen to key events. ");

                Subscriber<MapEvent> subscriber = eventsQueue::add;

                assetTree.registerSubscriber(NAME, MapEvent.class, subscriber);

                YamlLogging.writeMessage("unsubscribes to changes to the map");
                assetTree.unregisterSubscriber(NAME, subscriber);

                YamlLogging.writeMessage("puts the entry into the map, but since we have " +
                        "unsubscribed no event should be send form the server to the client");
                map.put("Hello", "World");

                Object object = eventsQueue.poll(500, MILLISECONDS);
                Assert.assertNull(object);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Test
    public void testSubscribeToKeyEvents() throws IOException, InterruptedException, InvalidSubscriberException {

        final BlockingQueue<String> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {

                YamlLogging.writeMessage("Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server");

                Subscriber<String> subscriber = eventsQueue::add;
                assetTree.registerSubscriber(NAME, String.class, subscriber);

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");
                map.put("Hello", "World");

                Object object = eventsQueue.poll(500, MILLISECONDS);

                Assert.assertEquals("Hello", object);

                // todo fix the text for the unsubscribe.
                assetTree.unregisterSubscriber(NAME, subscriber);
            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    @Test
    public void testSubscribeToValueBasedOnKeys() throws IOException, InterruptedException, InvalidSubscriberException {

        yamlLoggger(() -> {
            try {

                YamlLogging.writeMessage("Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server");

                ConcurrentMap<String, String> map = assetTree.acquireMap(NAME, String.class, String.class);

                map.put("Key-1", "Value-1");
                map.put("Key-2", "Value-2");

                assertEquals(2, map.size());

                ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(1);

                assetTree.registerSubscriber(NAME + "/Key-1?bootstrap=true", String.class, q::add);
                Asset asset = assetTree.getAsset(NAME + "/Key-1");

                assertTrue(asset.isSubAsset());
                Object take = q.poll(5, TimeUnit.SECONDS);

                Assert.assertEquals(take, "Value-1");
                map.put("Key-1", "Value-2");

                Object take2 = q.poll(5, TimeUnit.SECONDS);
                Assert.assertEquals(take2, "Value-2");

                assertEquals(2, map.size());

            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }

        });

    }

    //@Ignore("see https://higherfrequencytrading.atlassian.net/browse/CE-110")
    @Test
    public void testUnSubscribeToKeyEvents() throws IOException, InterruptedException {

        final BlockingQueue<String> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage("Sets up a subscription to listen to key events. Then " +
                        "unsubscribes and puts and entry into the map, no subsequent event should" +
                        " be received from the server");

                Subscriber<String> add = eventsQueue::add;
                assetTree.registerSubscriber(NAME, String.class, add);
                Thread.sleep(500);
                // need to unsubscribe the same object which was subscribed to.
                assetTree.unregisterSubscriber(NAME, add);

                eventsQueue.clear();

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");
                String expected = "World";
                map.getAndPut("Hello", expected);

                Object object = eventsQueue.poll(1000, MILLISECONDS);
                Assert.assertNull(object);

            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Test
    public void testSubscribeToKeyEventsAndRemoveKey() throws IOException, InterruptedException {

        final BlockingQueue<String> eventsQueue = new ArrayBlockingQueue<>(1024);

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage("Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map followed by a remove, notice " +
                        "that the " +
                        "'reply: Hello' is received twice, one for the put and one for the " +
                        "remove.");

                assetTree.registerSubscriber(NAME, String.class, eventsQueue::add);

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");
                String expected = "World";
                map.put("Hello", expected);
                map.remove("Hello");

                String putEvent = eventsQueue.poll(5, SECONDS);
                String removeEvent = eventsQueue.poll(5, SECONDS);

                Assert.assertTrue(putEvent instanceof String);
                Assert.assertTrue(removeEvent instanceof String);

            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Test
    public void testSubscribeToMapEventsAndRemoveKey() throws IOException, InterruptedException {

        final BlockingQueue<MapEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage("Sets up a subscription to listen to key events. And " +
                        "subsequently puts and entry into the map followed by a remove, notice " +
                        "that the " +
                        "'reply: Hello' is received twice, one for the put and one for the " +
                        "remove.");

                assetTree.registerSubscriber(NAME, MapEvent.class, eventsQueue::add);

                YamlLogging.writeMessage("puts an entry into the map so that an event will be " +
                        "triggered");
                String expected = "World";
                map.put("Hello", expected);
                Object putEvent = eventsQueue.poll(1, SECONDS);
                Assert.assertTrue(putEvent instanceof InsertedEvent);

                Thread.sleep(1);
                map.remove("Hello");

                Object removeEvent = eventsQueue.poll(5, SECONDS);
                Assert.assertTrue("event=" + removeEvent.getClass(), removeEvent instanceof RemovedEvent);

            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }
        });
    }
}

