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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.*;
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

        assertEquals("[InsertedEvent{assetName='/group-A', key=Key-1, value=Value-1}, " +
                "UpdatedEvent{assetName='/group-A', key=Key-1, oldValue=Value-1, value=Value-2}, " +
                "RemovedEvent{assetName='/group-A', key=Key-1, oldValue=Value-2}]", values.toString());
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
        reference.registerSubscriber(true, subscriber2);

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

        assertEquals("Message-3".length(), reference.apply(String::length), 0);
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
        MapView<String, String, String> map = acquireMap("group", String.class, String.class);
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
