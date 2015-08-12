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

package net.openhft.chronicle.engine.mit;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.KVSSubscription;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.IntStream;

@Ignore("Long running test")
public class RemoteSubscriptionModelPerformanceTest {

    //TODO DS test having the server side on another machine
    private static final int _noOfPuts = 50;
    private static final int _noOfRunsToAverage = Boolean.parseBoolean(System.getProperty("quick", "true")) ? 2 : 10;
    // TODO Fix so that it is 1 second. CHENT-49
    private static final long _secondInNanos = 9_000_000_000L;
    private static final AtomicInteger counter = new AtomicInteger();
    private static String _twoMbTestString;
    private static int _twoMbTestStringLength;
    private static Map<String, String> _testMap;
    private static VanillaAssetTree serverAssetTree, clientAssetTree;
    private static ServerEndpoint serverEndpoint;

    private final String _mapName = "PerfTestMap" + counter.incrementAndGet();

    @BeforeClass
    public static void setUpBeforeClass() throws IOException, URISyntaxException {
//        YamlLogging.showServerReads = true;
//        YamlLogging.clientReads = true;

        char[] chars = new char[2 << 20];
        Arrays.fill(chars, '~');
        _twoMbTestString = new String(chars);
        _twoMbTestStringLength = _twoMbTestString.length();

        serverAssetTree = new VanillaAssetTree(1).forTesting();
        //The following line doesn't add anything and breaks subscriptions
        serverAssetTree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore", VanillaMapView::new, KeyValueStore.class);
        serverAssetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.basePath(OS.TARGET).entries(_noOfPuts).averageValueSize(_twoMbTestStringLength), asset));
        TCPRegistry.createServerSocketChannelFor("RemoteSubscriptionModelPerformanceTest.port");
        serverEndpoint = new ServerEndpoint("RemoteSubscriptionModelPerformanceTest.port", serverAssetTree, WireType.BINARY);

        clientAssetTree = new VanillaAssetTree(13).forRemoteAccess("RemoteSubscriptionModelPerformanceTest.port", WireType.BINARY);
    }

    @Before
    public void setUp() throws IOException {
        Files.deleteIfExists(Paths.get(OS.TARGET, _mapName));

        _testMap = clientAssetTree.acquireMap(_mapName, String.class, String.class);

        _testMap.clear();
    }

    @After
    public void tearDown() throws IOException {
//        System.out.println("Native memory used "+OS.memory().nativeMemoryUsed());
//        System.gc();

    }

    @AfterClass
    public static void tearDownAfterClass() throws IOException {
        clientAssetTree.close();
        serverEndpoint.close();
        serverAssetTree.close();
        TCPRegistry.reset();
    }

    /**
     * Test that listening to events for a given key can handle 50 updates per second of 2 MB string values.
     */
    @Test
    public void testSubscriptionMapEventOnKeyPerformance() {
        String key = TestUtils.getKey(_mapName, 0);

        //Create subscriber and register
        //Add 4 for the number of puts that is added to the string
        TestChronicleKeyEventSubscriber keyEventSubscriber = new TestChronicleKeyEventSubscriber(_twoMbTestStringLength);

        clientAssetTree.registerSubscriber(_mapName + "/" + key + "?bootstrap=false&putReturnsNull=true", String.class, keyEventSubscriber);
        // TODO CHENT-49
        Jvm.pause(100);
        Asset child = serverAssetTree.getAsset(_mapName).getChild(key);
        Assert.assertNotNull(child);
        Subscription subscription = child.subscription(false);
        Assert.assertEquals(1, subscription.subscriberCount());

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(() -> {
            IntStream.range(0, _noOfPuts).forEach(i ->
            {
                _testMap.put(key, _twoMbTestString);
            });
        }, _noOfRunsToAverage, _secondInNanos);

        waitFor(() -> keyEventSubscriber.getNoOfEvents().get() < _noOfPuts * _noOfRunsToAverage * 0.2);

        //Test that the correct number of events was triggered on event listener
        Assert.assertEquals(_noOfPuts * _noOfRunsToAverage, keyEventSubscriber.getNoOfEvents().get());

        clientAssetTree.unregisterSubscriber(_mapName + "/" + key, keyEventSubscriber);

        Jvm.pause(100);
        Assert.assertEquals(0, subscription.subscriberCount());
    }

    /**
     * Test that listening to events for a given map can handle 50 updates per second of 2 MB string values and are
     * triggering events which contain both the key and value (topic).
     */
    @Test
    public void testSubscriptionMapEventOnTopicPerformance() {
        String key = TestUtils.getKey(_mapName, 0);

        //Create subscriber and register
        TestChronicleTopicSubscriber topicSubscriber = new TestChronicleTopicSubscriber(key, _twoMbTestStringLength);

        clientAssetTree.registerTopicSubscriber(_mapName, String.class, String.class, topicSubscriber);

        Jvm.pause(100);
        KVSSubscription subscription = (KVSSubscription) serverAssetTree.getAsset(_mapName).subscription(false);
        Assert.assertEquals(1, subscription.topicSubscriberCount());

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(i -> {
                    System.out.println("test");
                    int events = _noOfPuts * i;
                    waitFor(() -> events == topicSubscriber.getNoOfEvents().get());
                    Assert.assertEquals(events, topicSubscriber.getNoOfEvents().get());
                }, () -> {
                    IntStream.range(0, _noOfPuts).forEach(i ->
                    {
                        _testMap.put(key, _twoMbTestString);
                    });
                }, _noOfRunsToAverage, _secondInNanos
        );

        //Test that the correct number of events was triggered on event listener
        int events = _noOfPuts * _noOfRunsToAverage;
        waitFor(() -> events == topicSubscriber.getNoOfEvents().get());
        Assert.assertEquals(events, topicSubscriber.getNoOfEvents().get());

        clientAssetTree.unregisterTopicSubscriber(_mapName, topicSubscriber);
        waitFor(() -> 0 == subscription.topicSubscriberCount());
        Assert.assertEquals(0, subscription.topicSubscriberCount());
    }

    /**
     * Tests the performance of an event listener on the map for Insert events of 2 MB strings.
     * Expect it to handle at least 50 2 MB updates per second.
     */
    @Test
    public void testSubscriptionMapEventListenerInsertPerformance() {
//        YamlLogging.showServerReads = YamlLogging.showServerWrites = true;
//        YamlLogging.clientWrites = true;
        //Create subscriber and register
        TestChronicleMapEventListener mapEventListener = new TestChronicleMapEventListener(_mapName, _twoMbTestStringLength);

        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(mapEventListener);
        clientAssetTree.registerSubscriber(_mapName, MapEvent.class, mapEventSubscriber);

        Jvm.pause(100);
        KVSSubscription subscription = (KVSSubscription) serverAssetTree.getAsset(_mapName).subscription(false);
        Assert.assertEquals(1, subscription.entrySubscriberCount());

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(i -> {
                    if (i > 0) {
                        waitFor(() -> mapEventListener.getNoOfInsertEvents().get() >= _noOfPuts);
                        Assert.assertEquals(_noOfPuts, mapEventListener.getNoOfInsertEvents().get());
                    }
                    //Test that the correct number of events were triggered on event listener
                    Assert.assertEquals(0, mapEventListener.getNoOfRemoveEvents().get());
                    Assert.assertEquals(0, mapEventListener.getNoOfUpdateEvents().get());

                    _testMap.clear();

                    mapEventListener.resetCounters();
                }, () -> {
                    IntStream.range(0, _noOfPuts).forEach(i ->
                    {
                        _testMap.put(TestUtils.getKey(_mapName, i), _twoMbTestString);
                    });
                }, _noOfRunsToAverage, _secondInNanos
        );

        clientAssetTree.unregisterSubscriber(_mapName, mapEventSubscriber);

        Jvm.pause(1000);
        Assert.assertEquals(0, subscription.entrySubscriberCount());
    }

    /**
     * Tests the performance of an event listener on the map for Update events of 2 MB strings.
     * Expect it to handle at least 50 2 MB updates per second.
     */
    @Test
    public void testSubscriptionMapEventListenerUpdatePerformance() {
        //Put values before testing as we want to ignore the insert events
        Function<Integer, Object> putFunction = a -> _testMap.put(TestUtils.getKey(_mapName, a), _twoMbTestString);

        IntStream.range(0, _noOfPuts).forEach(i ->
        {
            putFunction.apply(i);
        });

        Jvm.pause(100);
        //Create subscriber and register
        TestChronicleMapEventListener mapEventListener = new TestChronicleMapEventListener(_mapName, _twoMbTestStringLength);

        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(mapEventListener);
        clientAssetTree.registerSubscriber(_mapName + "?bootstrap=false", MapEvent.class, mapEventSubscriber);

        KVSSubscription subscription = (KVSSubscription) serverAssetTree.getAsset(_mapName).subscription(false);

        waitFor(() -> subscription.entrySubscriberCount() == 1);
        Assert.assertEquals(1, subscription.entrySubscriberCount());

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(i -> {
                    if (i > 0) {
                        waitFor(() -> mapEventListener.getNoOfUpdateEvents().get() >= _noOfPuts);

                        //Test that the correct number of events were triggered on event listener
                        Assert.assertEquals(_noOfPuts, mapEventListener.getNoOfUpdateEvents().get());
                    }
                    Assert.assertEquals(0, mapEventListener.getNoOfInsertEvents().get());
                    Assert.assertEquals(0, mapEventListener.getNoOfRemoveEvents().get());

                    mapEventListener.resetCounters();

                }, () -> {
                    IntStream.range(0, _noOfPuts).forEach(i ->
                    {
                        putFunction.apply(i);
                    });
                }, _noOfRunsToAverage, _secondInNanos
        );
        clientAssetTree.unregisterSubscriber(_mapName, mapEventSubscriber);

        waitFor(() -> subscription.entrySubscriberCount() == 0);
        Assert.assertEquals(0, subscription.entrySubscriberCount());
    }

    private void waitFor(BooleanSupplier b) {
        for (int i = 1; i <= 40; i++)
            if (!b.getAsBoolean())
                Jvm.pause(i * i);
    }

    /**
     * Tests the performance of an event listener on the map for Remove events of 2 MB strings.
     * Expect it to handle at least 50 2 MB updates per second.
     */
    @Test
    public void testSubscriptionMapEventListenerRemovePerformance() {
        //Put values before testing as we want to ignore the insert and update events

        //Create subscriber and register
        TestChronicleMapEventListener mapEventListener = new TestChronicleMapEventListener(_mapName, _twoMbTestStringLength);

        Subscriber<MapEvent> mapEventSubscriber = e -> e.apply(mapEventListener);
        clientAssetTree.registerSubscriber(_mapName, MapEvent.class, mapEventSubscriber);

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        long runtimeInNanos = 0;

        for (int i = 0; i < _noOfRunsToAverage; i++) {
            //Put values before testing as we want to ignore the insert and update events
            IntStream.range(0, _noOfPuts).forEach(c ->
            {
                _testMap.put(TestUtils.getKey(_mapName, c), _twoMbTestString);
            });
            waitFor(() -> mapEventListener.getNoOfInsertEvents().get() >= _noOfPuts);

            mapEventListener.resetCounters();

            long startTime = System.nanoTime();

            IntStream.range(0, _noOfPuts).forEach(c ->
            {
                _testMap.remove(TestUtils.getKey(_mapName, c));
            });

            runtimeInNanos += System.nanoTime() - startTime;
            waitFor(() -> mapEventListener.getNoOfRemoveEvents().get() >= _noOfPuts);

            //Test that the correct number of events were triggered on event listener
            Assert.assertEquals(0, mapEventListener.getNoOfInsertEvents().get());
            Assert.assertEquals(_noOfPuts, mapEventListener.getNoOfRemoveEvents().get());
            Assert.assertEquals(0, mapEventListener.getNoOfUpdateEvents().get());
        }

        Assert.assertTrue((runtimeInNanos / (_noOfPuts * _noOfRunsToAverage)) <= _secondInNanos);
        clientAssetTree.unregisterSubscriber(_mapName, mapEventSubscriber);
    }

    /**
     * Checks that all updates triggered are for the key specified in the constructor and increments the number of
     * updates.
     */
    class TestChronicleKeyEventSubscriber implements Subscriber<String> {
        private int _stringLength;
        private AtomicInteger _noOfEvents = new AtomicInteger(0);

        public TestChronicleKeyEventSubscriber(int stringLength) {
            _stringLength = stringLength;
        }

        public AtomicInteger getNoOfEvents() {
            return _noOfEvents;
        }

        @Override
        public void onMessage(String newValue) {
            Assert.assertEquals(_stringLength, newValue.length());
            _noOfEvents.incrementAndGet();
        }
    }

    /**
     * Topic subscriber checking for each message that it is for the right key (in constructor) and the expected size
     * value.
     * Increments event counter which can be checked at the end of the test.
     */
    class TestChronicleTopicSubscriber implements TopicSubscriber<String, String> {
        private String _keyName;
        private int _stringLength;
        private AtomicInteger _noOfEvents = new AtomicInteger(0);

        public TestChronicleTopicSubscriber(String keyName, int stringLength) {
            _keyName = keyName;
            _stringLength = stringLength;
        }

        /**
         * Test that the topic/key is the one specified in constructor and the message is the expected size.
         *
         * @throws InvalidSubscriberException
         */
        @Override
        public void onMessage(String topic, String message) throws InvalidSubscriberException {
            Assert.assertEquals(_keyName, topic);
            Assert.assertEquals(_stringLength, message.length());

            _noOfEvents.incrementAndGet();
        }

        public AtomicInteger getNoOfEvents() {
            return _noOfEvents;
        }
    }

    /**
     * Map event listener for performance testing. Checks that the key is the one expected and the size of the value is
     * as expected.
     * Increments event specific counters that can be used to check agains the expected number of events.
     */
    class TestChronicleMapEventListener implements MapEventListener<String, String> {
        private AtomicInteger _noOfInsertEvents = new AtomicInteger(0);
        private AtomicInteger _noOfUpdateEvents = new AtomicInteger(0);
        private AtomicInteger _noOfRemoveEvents = new AtomicInteger(0);

        private String _mapName;
        private int _stringLength;

        public TestChronicleMapEventListener(String mapName, int stringLength) {
            _mapName = mapName;
            _stringLength = stringLength;
        }

        @Override
        public void update(String assetName, String key, String oldValue, String newValue) {
            testKeyAndValue(key, newValue, _noOfUpdateEvents);
        }

        @Override
        public void insert(String assetName, String key, String value) {
            testKeyAndValue(key, value, _noOfInsertEvents);
        }

        @Override
        public void remove(String assetName, String key, String value) {
            testKeyAndValue(key, value, _noOfRemoveEvents);
        }

        public AtomicInteger getNoOfInsertEvents() {
            return _noOfInsertEvents;
        }

        public AtomicInteger getNoOfUpdateEvents() {
            return _noOfUpdateEvents;
        }

        public AtomicInteger getNoOfRemoveEvents() {
            return _noOfRemoveEvents;
        }

        public void resetCounters() {
            _noOfInsertEvents = new AtomicInteger(0);
            _noOfUpdateEvents = new AtomicInteger(0);
            _noOfRemoveEvents = new AtomicInteger(0);
        }

        private void testKeyAndValue(String key, String value, AtomicInteger counterToIncrement) {
            int counter = counterToIncrement.getAndIncrement();
            Assert.assertEquals(TestUtils.getKey(_mapName, counter), key);
            Assert.assertEquals(_stringLength, value.length());
        }
    }
}