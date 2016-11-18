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

package net.openhft.chronicle.engine.mit;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import org.junit.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static sun.jvm.hotspot.runtime.VMOps.ThreadDump;

@Ignore
public class SubscriptionModelFilePerKeyPerformanceTest {
    static final AtomicInteger counter = new AtomicInteger();

    private static final int _noOfPuts = 50;
    private static final int _noOfRunsToAverage = Boolean.getBoolean("quick") ? 2 : 10;
    private static final long _secondInNanos = Jvm.isDebug() ? 1_200_000_000L : 1_000_000_000L;
    private static String _twoMbTestString;
    private static int _twoMbTestStringLength;
    private static Map<String, String> _testMap;
    private static String _mapName = "PerfTestMap";
    private ThreadDump threadDump;

    @BeforeClass
    public static void setUpBeforeClass() {
        char[] chars = new char[2 << 20];
        Arrays.fill(chars, '~');
        _twoMbTestString = new String(chars);
        _twoMbTestStringLength = _twoMbTestString.length();
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Before
    public void setUp() {
        Chassis.resetChassis();

        ((VanillaAsset) Chassis.assetTree().root()).enableTranslatingValuesToBytesStore();

        String basePath = OS.TARGET + "/fpk/" + counter.getAndIncrement();
        System.out.println("Writing to " + basePath);
        Chassis.assetTree().root().addLeafRule(AuthenticatedKeyValueStore.class, "FilePer Key",
                (context, asset) -> new FilePerKeyValueStore(context.basePath(basePath), asset));
        _testMap = Chassis.acquireMap(_mapName, String.class, String.class);

        _testMap.clear();
    }

    @After
    public void tearDown() throws IOException {
//        System.out.println("Native memory used "+OS.memory().nativeMemoryUsed());
        ((Closeable) ((MapView) _testMap).underlying()).close();
        System.gc();
    }

    /**
     * Test that listening to events for a given key can handle 50 updates per second of 2 MB string values.
     */
    @Test
    public void testSubscriptionMapEventOnKeyPerformance() {
        String key = TestUtils.getKey(_mapName, 0);

        //Create subscriber and register
        TestChronicleKeyEventSubscriber keyEventSubscriber = new TestChronicleKeyEventSubscriber(_twoMbTestStringLength);

        Chassis.registerSubscriber(_mapName + "?bootstrap=false", MapEvent.class, me -> keyEventSubscriber.onMessage((String) me.getValue()));

        AtomicInteger counter = new AtomicInteger();
        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(i ->
                        keyEventSubscriber.waitForEvents(_noOfPuts * i, 0.1),
                () -> IntStream.range(0, _noOfPuts).forEach(
                        i -> _testMap.put(key + i, counter.incrementAndGet() + _twoMbTestString)),
                _noOfRunsToAverage, _secondInNanos);

        //Test that the correct number of events was triggered on event listener
        keyEventSubscriber.waitForEvents(_noOfPuts * _noOfRunsToAverage, 0.3);
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

        Chassis.registerTopicSubscriber(_mapName, String.class, String.class, topicSubscriber);

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(() -> {
            IntStream.range(0, _noOfPuts).forEach(i ->
            {
                _testMap.put(key, i + _twoMbTestString);
                _testMap.size();
            });
        }, _noOfRunsToAverage, _secondInNanos);

        //Test that the correct number of events was triggered on event listener
        topicSubscriber.waitForEvents(_noOfPuts * _noOfRunsToAverage, 0.7);
    }

    /**
     * Tests the performance of an event listener on the map for Insert events of 2 MB strings.
     * Expect it to handle at least 50 2 MB updates per second.
     */
    @Test
    public void testSubscriptionMapEventListenerInsertPerformance() {
        //Create subscriber and register
        TestChronicleMapEventListener mapEventListener = new TestChronicleMapEventListener(_mapName, _twoMbTestStringLength);
        Chassis.registerSubscriber(_mapName, MapEvent.class, e -> e.apply(mapEventListener));

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(i -> {
            _testMap.clear();
            Jvm.pause(500);
            mapEventListener.resetCounters();
        }, () -> {
            IntStream.range(0, _noOfPuts).forEach(i ->
            {
                _testMap.put(TestUtils.getKey(_mapName, i), _twoMbTestString);
            });
            mapEventListener.waitForNMaps(_noOfPuts);

            //Test that the correct number of events were triggered on event listener
            if (_noOfPuts != mapEventListener.getNoOfInsertEvents().get())
                Jvm.pause(50);
            Assert.assertEquals(_noOfPuts, mapEventListener.getNoOfInsertEvents().get());
            Assert.assertEquals(0, mapEventListener.getNoOfRemoveEvents().get());
            Assert.assertEquals(0, mapEventListener.getNoOfUpdateEvents().get());
        }, _noOfRunsToAverage, _secondInNanos);
    }

    /**
     * Tests the performance of an event listener on the map for Update events of 2 MB strings.
     * Expect it to handle at least 50 2 MB updates per second.
     */
    @Test
    public void testSubscriptionMapEventListenerUpdatePerformance() {
        //Put values before testing as we want to ignore the insert events
        Function<Integer, Object> putFunction = a -> _testMap.put(TestUtils.getKey(_mapName, a), System.nanoTime() + _twoMbTestString);

        IntStream.range(0, _noOfPuts).parallel().forEach(i ->
        {
            putFunction.apply(i);
        });

        //Create subscriber and register
        TestChronicleMapEventListener mapEventListener = new TestChronicleMapEventListener(_mapName, _twoMbTestStringLength);

        Chassis.registerSubscriber(_mapName + "?bootstrap=false", MapEvent.class, e -> e.apply(mapEventListener));

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        TestUtils.runMultipleTimesAndVerifyAvgRuntime(i -> {
            Jvm.pause(200);
            mapEventListener.resetCounters();
        }, () -> {
            IntStream.range(0, _noOfPuts).forEach(i ->
            {
                putFunction.apply(i);
            });
            mapEventListener.waitForNMaps(_noOfPuts);

            //Test that the correct number of events were triggered on event listener
            // todo make more reliable on windows.
            Assert.assertEquals(_noOfPuts, mapEventListener.getNoOfUpdateEvents().get()
                    + mapEventListener.getNoOfInsertEvents().get(), _noOfPuts * 0.4);
            Assert.assertEquals(0, mapEventListener.getNoOfRemoveEvents().get());

        }, _noOfRunsToAverage, _secondInNanos);
    }

    /**
     * Tests the performance of an event listener on the map for Remove events of 2 MB strings.
     * Expect it to handle at least 50 2 MB updates per second.
     */
    @Test
    public void testSubscriptionMapEventListenerRemovePerformance() throws InterruptedException {
        //Put values before testing as we want to ignore the insert and update events

        //Create subscriber and register
        TestChronicleMapEventListener mapEventListener = new TestChronicleMapEventListener(_mapName, _twoMbTestStringLength);

        Chassis.registerSubscriber(_mapName + "?bootstrap=false", MapEvent.class, e -> e.apply(mapEventListener));

        //Perform test a number of times to allow the JVM to warm up, but verify runtime against average
        long runtimeInNanos = 0;

        for (int i = 0; i < _noOfRunsToAverage; i++) {
            Jvm.pause(400);
            mapEventListener.resetCounters();

            //Put values before testing as we want to ignore the insert and update events
            IntStream.range(0, _noOfPuts).forEach(c ->
            {
                _testMap.put(TestUtils.getKey(_mapName, c), _twoMbTestString);
            });

            mapEventListener.waitForNMaps(_noOfPuts);

            mapEventListener.resetCounters();

            long startTime = System.nanoTime();

            IntStream.range(0, _noOfPuts).parallel().forEach(c ->
            {
                _testMap.remove(TestUtils.getKey(_mapName, c));
            });

            mapEventListener.waitForNMaps(_noOfPuts);
            runtimeInNanos += System.nanoTime() - startTime;

            //Test that the correct number of events were triggered on event listener
            Assert.assertEquals(0, mapEventListener.getNoOfInsertEvents().get());
            Assert.assertEquals(_noOfPuts, mapEventListener.getNoOfRemoveEvents().get());
            Assert.assertEquals(0, mapEventListener.getNoOfUpdateEvents().get(), 1);
        }

        Assert.assertTrue((runtimeInNanos / (_noOfPuts * _noOfRunsToAverage)) <= _secondInNanos);
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
            try {
                Assert.assertEquals(_stringLength + 2, newValue.length(), 1);
            } catch (Error e) {
                throw e;
            }
            _noOfEvents.incrementAndGet();
        }

        public void waitForEvents(int events, double error) {
            for (int i = 1; i <= 30; i++) {
                if (events * (1 - error / 2) <= getNoOfEvents().get())
                    break;
                Jvm.pause(i * i);
            }
            Jvm.pause(100);
            Assert.assertEquals(events * (1 - error / 2), getNoOfEvents().get(), error / 2 * events);
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
            if (message == null) {
                System.out.println("topic " + topic + " deleted?");
                return;
            }
            Assert.assertEquals(_keyName, topic);
            Assert.assertEquals(_stringLength + 2, message.length(), 1);

            _noOfEvents.incrementAndGet();
        }

        public AtomicInteger getNoOfEvents() {
            return _noOfEvents;
        }

        public void waitForEvents(int events, double error) {
            for (int i = 1; i <= 30; i++) {
                if (events * (1 - error) <= getNoOfEvents().get())
                    break;
                Jvm.pause(i * i);
            }
            Jvm.pause(100);
            Assert.assertEquals(events * (1 - error / 2), getNoOfEvents().get(), error / 2 * events);
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
        private Set<String> mapsUpdated = Collections.synchronizedSet(new TreeSet<>());

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
            mapsUpdated.clear();
        }

        private void testKeyAndValue(String key, String value, AtomicInteger counterToIncrement) {
//            System.out.println("key: " + key);
            counterToIncrement.getAndIncrement();
            mapsUpdated.add(key);
            try {
                if (value != null)
                    Assert.assertEquals(_stringLength + 8, value.length(), 8);
            } catch (Error e) {
                throw e;
            }
        }

        public void waitForNMaps(int noOfMaps) {
            for (int i = 1; i <= 40; i++) {
                if (mapsUpdated.size() >= noOfMaps)
                    break;
                Jvm.pause(i * i);
            }
            Assert.assertEquals(toString(), noOfMaps, mapsUpdated.size());
        }

        @Override
        public String toString() {
            return "TestChronicleMapEventListener{" +
                    "_noOfInsertEvents=" + _noOfInsertEvents +
                    ", _noOfUpdateEvents=" + _noOfUpdateEvents +
                    ", _noOfRemoveEvents=" + _noOfRemoveEvents +
                    ", mapsUpdated=" + mapsUpdated.size() +
                    ", _mapName='" + _mapName + '\'' +
                    ", _stringLength=" + _stringLength +
                    '}';
        }
    }
}