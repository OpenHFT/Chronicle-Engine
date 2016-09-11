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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.openhft.chronicle.engine.Chassis.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 22/05/15.
 */
public class ChassisRFCTest {

    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }
    @Before
    public void setUpTest() {
        Chassis.resetChassis();
    }

    @Test
    public void subscriptionToATopic() {
        Map<String, String> map = acquireMap("group-A", String.class, String.class);

        map.put("Key-1", "Value-1");

        List<String> values = new ArrayList<>();
        Subscriber<String> subscriber = values::add;
        registerSubscriber("group-A/Key-1?bootstrap=true", String.class, subscriber);

        map.put("Key-1", "Value-2");
        map.remove("Key-1");

        assertEquals("[Value-1, Value-2, null]", values.toString());
    }

    @Test
    public void subscriptionToAGroupOfTopics() {
        Map<String, String> map = acquireMap("group-A", String.class, String.class);

        map.put("Key-1", "Value-1");

        List<String> values = new ArrayList<>();
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add("{name: " + topic + ", message: " + message + "}");
        registerTopicSubscriber("group-A", String.class, String.class, subscriber);

        map.put("Key-1", "Value-2");
        map.remove("Key-1");

        assertEquals("[{name: Key-1, message: Value-1}, " +
                        "{name: Key-1, message: Value-2}, " +
                        "{name: Key-1, message: null}]",
                values.toString());
    }

    @Test
    public void subscriptionToChangesInEntries() {
        Map<String, String> map = acquireMap("group-A", String.class, String.class);

        map.put("Key-1", "Value-1");

        List<MapEvent> values = new ArrayList<>();
        Subscriber<MapEvent> subscriber = values::add;
        registerSubscriber("group-A?view=map&bootstrap=true", MapEvent.class, subscriber);

        map.put("Key-1", "Value-2");
        map.remove("Key-1");

        assertEquals("[!InsertedEvent {\n" +
                "  assetName: /group-A,\n" +
                "  key: Key-1,\n" +
                "  value: Value-1,\n" +
                "  isReplicationEvent: false\n" +
                "}\n" +
                ", !UpdatedEvent {\n" +
                "  assetName: /group-A,\n" +
                "  key: Key-1,\n" +
                "  oldValue: Value-1,\n" +
                "  value: Value-2,\n" +
                "  isReplicationEvent: false,\n" +
                "  hasValueChanged: true\n" +
                "}\n" +
                ", !RemovedEvent {\n" +
                "  assetName: /group-A,\n" +
                "  key: Key-1,\n" +
                "  oldValue: Value-2,\n" +
                "  isReplicationEvent: false\n" +
                "}\n" +
                "]", values.toString());
    }

    @Test
    public void publishToATopic() {
        Map<String, String> map = acquireMap("group", String.class, String.class);
        Publisher<String> publisher = acquirePublisher("group/topic", String.class);
        List<String> values = new ArrayList<>();
        Subscriber<String> subscriber = values::add;
        registerSubscriber("group/topic?bootstrap=false", String.class, subscriber);

        publisher.publish("Message-1");
        assertEquals("Message-1", map.get("topic"));

        map.put("topic", "Message-2");
        assertEquals("Message-2", map.get("topic"));
        assertEquals("[Message-1, Message-2]", values.toString());
    }

    @Test
    public void referenceToATopic() {
        Map<String, String> map = acquireMap("group", String.class, String.class);
        Reference<String> reference = acquireReference("group/topic", String.class);

        List<String> values = new ArrayList<>();
        Subscriber<String> subscriber = values::add;
        registerSubscriber("group/topic?bootstrap=false", String.class, subscriber);

        List<String> values2 = new ArrayList<>();
        Subscriber<String> subscriber2 = values2::add;
        reference.registerSubscriber(true, 0, subscriber2);

        reference.set("Message-1");
        assertEquals("Message-1", reference.get());

        assertEquals("Message-1", map.get("topic"));

        reference.publish("Message-2");
        assertEquals("Message-2", reference.get());
        assertEquals("Message-2", map.get("topic"));
        assertEquals("[Message-1, Message-2]", values.toString());
        assertEquals("[null, Message-1, Message-2]", values2.toString());

        reference.set("Message-3");
        assertEquals("Message-3", reference.get());
        assertEquals("Message-3", map.get("topic"));
        assertEquals("[Message-1, Message-2, Message-3]", values.toString());
        assertEquals("[null, Message-1, Message-2, Message-3]", values2.toString());

        assertEquals("Message-3".length(), reference.applyTo(String::length), 0);
        reference.asyncUpdate(String::toUpperCase);
        assertEquals("MESSAGE-3", reference.get());
        assertEquals("MESSAGE-3", map.get("topic"));
        assertEquals("[Message-1, Message-2, Message-3, MESSAGE-3]", values.toString());
        assertEquals("[null, Message-1, Message-2, Message-3, MESSAGE-3]", values2.toString());

        assertEquals("Message-3A".length(), reference.syncUpdate(s -> s.concat("A"), String::length), 0);
        assertEquals("MESSAGE-3A", reference.get());
        assertEquals("MESSAGE-3A", map.get("topic"));
        assertEquals("[Message-1, Message-2, Message-3, MESSAGE-3, MESSAGE-3A]", values.toString());
        assertEquals("[null, Message-1, Message-2, Message-3, MESSAGE-3, MESSAGE-3A]", values2.toString());
    }

    @Test
    public void publishToAnyTopicInAGroup() {
        Map<String, String> map = acquireMap("group", String.class, String.class);
        map.clear();
        TopicPublisher<String, String> publisher = acquireTopicPublisher("group", String.class, String.class);
        List<String> values = new ArrayList<>();
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add("{name: " + topic + ", message: " + message + "}");
        registerTopicSubscriber("group", String.class, String.class, subscriber);

        List<String> values2 = new ArrayList<>();
        TopicSubscriber<String, String> subscriber2 = (topic, message) -> values2.add("{name: " + topic + ", message: " + message + "}");
        publisher.registerTopicSubscriber(subscriber2);

        publisher.publish("topic-1", "Message-1");
        assertEquals("Message-1", map.get("topic-1"));

        publisher.publish("topic-1", "Message-2");
        assertEquals("Message-2", map.get("topic-1"));
        assertEquals("[{name: topic-1, message: Message-1}, {name: topic-1, message: Message-2}]", values2.toString());
        assertEquals("[{name: topic-1, message: Message-1}, {name: topic-1, message: Message-2}]", values.toString());
    }

    @Test
    public void updateTheMapView() {
        MapView<String, String> map = acquireMap("group", String.class, String.class);
        List<String> values = new ArrayList<>();
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add("{name: " + topic + ", message: " + message + "}");
        registerTopicSubscriber("group", String.class, String.class, subscriber);

        map.put("topic-1", "Message-1");
        assertEquals("Message-1", map.get("topic-1"));

        assertEquals(1, map.applyTo(Map::size), 0);

        map.remove("topic-1");
        assertEquals(null, map.get("topic-1"));
        assertEquals("[{name: topic-1, message: Message-1}, {name: topic-1, message: null}]", values.toString());

        map.asyncUpdate(m -> map.put("topic-2", "Message-2"));
        assertEquals("Message-2", map.get("topic-2"));
        assertEquals("[{name: topic-1, message: Message-1}, {name: topic-1, message: null}, {name: topic-2, message: Message-2}]", values.toString());

        assertEquals(0, map.syncUpdate(m -> m.remove("topic-2"), Map::size), 0);
        assertEquals(null, map.get("topic-2"));
        assertEquals("[{name: topic-1, message: Message-1}, {name: topic-1, message: null}, {name: topic-2, message: Message-2}, {name: topic-2, message: null}]", values.toString());
    }
}
