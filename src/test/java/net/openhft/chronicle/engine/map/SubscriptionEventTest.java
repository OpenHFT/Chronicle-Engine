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
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.server.WireType;
import net.openhft.chronicle.engine.tree.*;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static net.openhft.chronicle.engine.Utils.methodName;
import static net.openhft.chronicle.engine.Utils.yamlLoggger;
import static net.openhft.chronicle.engine.server.WireType.wire;
import static org.easymock.EasyMock.*;

/**
 * test using the listener both remotely or locally via the engine
 *
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class SubscriptionEventTest extends ThreadMonitoringTest {
    private static ConcurrentMap<String, String> map;
    private static final String NAME = "test";

    private static Boolean isRemote;

    private AssetTree assetTree = new VanillaAssetTree().forTesting();
    @NotNull
    @Rule
    public TestName name = new TestName();
    private VanillaAssetTree serverAssetTree;
    private ServerEndpoint serverEndpoint;

    @Before
    public void before() throws IOException {
        serverAssetTree = new VanillaAssetTree().forTesting();

        if (isRemote) {

            methodName(name.getMethodName());

            serverEndpoint = new ServerEndpoint(serverAssetTree);

            assetTree = new VanillaAssetTree().forRemoteAccess("localhost", serverEndpoint.getPort());
        } else
            assetTree = serverAssetTree;

        map = assetTree.acquireMap(NAME, String.class, String.class);
    }

    @After
    public void after() throws IOException {
        assetTree.close();
        if (serverEndpoint != null)
            serverEndpoint.close();
        serverAssetTree.close();
        if (map instanceof Closeable)
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

    public SubscriptionEventTest(Object isRemote, Object wireType) {
        SubscriptionEventTest.isRemote = (Boolean) isRemote;

        wire = (Function<Bytes, Wire>) wireType;
    }

    @Test
    public void testSubscribeToChangesToTheMap() throws IOException, InterruptedException {

        final BlockingQueue<MapEvent> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server";

                Subscriber<MapEvent> add = eventsQueue::add;
                assetTree.registerSubscriber(NAME, MapEvent.class, add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                map.put("Hello", "World");

                Object object = eventsQueue.take();
                Assert.assertTrue(object instanceof InsertedEvent);

                assetTree.unregisterSubscriber(NAME, add);
            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Test
    @Ignore("TODO")
    public void testTopicSubscribe() throws InvalidSubscriberException {

        class TopicDetails<T, M> {
            private final M message;
            private final T topic;

            public TopicDetails(final T topic, final M message) {
                this.topic = topic;
                this.message = message;
            }

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
                YamlLogging.writeMessage = "Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server";

                assetTree.registerTopicSubscriber(NAME, String.class, String.class,
                        (topic, message) -> eventsQueue.add(new TopicDetails(topic, message)));

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                map.put("Hello", "World");

                TopicDetails take = eventsQueue.take();
                System.out.println(take);

                // todo fix the text for the unsubscribe.
                //  assetTree.unregisterTopicSubscriber(NAME, subscriber);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
        //  waitFor(subscriber);
    }

    @Test
    @Ignore("TODO")
    public void testTopicSubscribeToChangesToTheMapMock() throws InvalidSubscriberException {

        TopicSubscriber<String, String> subscriber = createMock(TopicSubscriber.class);
        subscriber.onMessage("Hello", "World");
        subscriber.onEndOfSubscription();
        replay(subscriber);

        yamlLoggger(() -> {
            try {
                // todo fix the text
                YamlLogging.writeMessage = "Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server";

                assetTree.registerTopicSubscriber(NAME, String.class, String.class, subscriber);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                map.put("Hello", "World");

                // todo fix the text for the unsubscribe.
                assetTree.unregisterTopicSubscriber(NAME, subscriber);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
        waitFor(subscriber);
    }

    @Test
    @Ignore("TODO")
    public void testTopologicalEventsMock() throws InvalidSubscriberException {

        Subscriber<TopologicalEvent> subscriber = createMock(Subscriber.class);
        subscriber.onMessage(ExistingAssetEvent.of("/", NAME));
        subscriber.onMessage(AddedAssetEvent.of("/", "group"));
        subscriber.onMessage(AddedAssetEvent.of("/group", NAME));
        subscriber.onMessage(AddedAssetEvent.of("/group", NAME + 2));
        subscriber.onMessage(RemovedAssetEvent.of("/group", NAME));
        subscriber.onEndOfSubscription();
        replay(subscriber);

        yamlLoggger(() -> {
            try {
                // todo fix the text
                YamlLogging.writeMessage = "Sets up a subscription to listen to map events. And " +
                        "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                        "received from the server";

                assetTree.registerSubscriber(NAME, TopologicalEvent.class, subscriber);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";

                assetTree.acquireMap("/group/" + NAME, String.class, String.class);
                assetTree.acquireMap("/group/" + NAME + 2, String.class, String.class);
                Jvm.pause(50);
                // the client cannot remove maps yet.
                serverAssetTree.acquireAsset(RequestContext.requestContext("/group")).removeChild(NAME);

                assetTree.unregisterSubscriber(NAME, subscriber);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
        waitFor(subscriber);
    }

    static void waitFor(Object subscriber) {
        for (int i = 1; i < 10; i++) {
            Jvm.pause(i);
            try {
                verify(subscriber);
            } catch (AssertionError e) {
                // retry
            }
        }
        verify(subscriber);
    }

    @Test
    public void testUnsubscribeToMapEvents() throws IOException, InterruptedException {

        final BlockingQueue<MapEvent> eventsQueue = new LinkedBlockingQueue<>();

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

                Object object = eventsQueue.poll(100, MILLISECONDS);
                Assert.assertNull(object);

            } catch (Exception e) {
                throw Jvm.rethrow(e);
            }
        });
    }

    @Test
    @Ignore("TODO")
    public void testSubscribeToKeyEvents() throws IOException, InterruptedException, InvalidSubscriberException {

        Subscriber<String> subscriber = createMock(Subscriber.class);
        subscriber.onMessage("Hello");
        subscriber.onEndOfSubscription();
        replay(subscriber);

        yamlLoggger(() -> {

            YamlLogging.writeMessage = "Sets up a subscription to listen to key events. And " +
                    "subsequently puts and entry into the map, notice that the InsertedEvent is " +
                    "received from the server";

            assetTree.registerSubscriber(NAME, String.class, subscriber);

            YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                    "triggered";
            map.put("Hello", "World");

            // todo fix the text for the unsubscribe.
            assetTree.unregisterSubscriber(NAME, subscriber);

        });
        waitFor(subscriber);
    }

    @Test
    public void testUnSubscribeToKeyEvents() throws IOException, InterruptedException {

        final BlockingQueue<String> eventsQueue = new LinkedBlockingQueue<>();

        yamlLoggger(() -> {
            try {
                YamlLogging.writeMessage = "Sets up a subscription to listen to key events. Then " +
                        "unsubscribes and puts and entry into the map, no subsequent event should" +
                        " be received from the server";

                Subscriber<String> add = eventsQueue::add;
                assetTree.registerSubscriber(NAME, String.class, add);
                // need to unsubscribe the same object which was subscribed to.
                assetTree.unregisterSubscriber(NAME, add);

                YamlLogging.writeMessage = "puts an entry into the map so that an event will be " +
                        "triggered";
                String expected = "World";
                map.put("Hello", expected);

                Object object = eventsQueue.poll(100, MILLISECONDS);
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

                String putEvent = eventsQueue.poll(100, MILLISECONDS);
                String removeEvent = eventsQueue.poll(100, MILLISECONDS);
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

                Object putEvent = eventsQueue.poll(100, MILLISECONDS);
                Object removeEvent = eventsQueue.poll(100, MILLISECONDS);
                Assert.assertTrue(putEvent instanceof InsertedEvent);
                Assert.assertTrue(removeEvent instanceof RemovedEvent);

            } catch (InterruptedException e) {
                throw Jvm.rethrow(e);
            }
        });
    }
}



