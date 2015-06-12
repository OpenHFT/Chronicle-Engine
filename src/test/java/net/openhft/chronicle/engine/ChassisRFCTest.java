package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.map.MapEvent;
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
        Subscriber<String> subscriber = (String e) -> {
            values.add(e);
        };
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
        reference.registerSubscriber(subscriber2);

        reference.set("Message-1");
        assertEquals("Message-1", reference.get());

        assertEquals("Message-1", map.get("topic"));

        reference.publish("Message-2");
        assertEquals("Message-2", reference.get());
        assertEquals("Message-2", map.get("topic"));
        assertEquals("[Message-1, Message-2]", values.toString());
        assertEquals("[null, Message-1, Message-2]", values2.toString());
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
        Map<String, String> map = acquireMap("group", String.class, String.class);
        List<String> values = new ArrayList<>();
        TopicSubscriber<String, String> subscriber = (topic, message) -> values.add("{name: " + topic + ", message: " + message + "}");
        registerTopicSubscriber("group", String.class, String.class, subscriber);

        map.put("topic-1", "Message-1");
        assertEquals("Message-1", map.get("topic-1"));

        map.remove("topic-1");
        assertEquals(null, map.get("topic-1"));
        assertEquals("[{name: topic-1, message: Message-1}, {name: topic-1, message: null}]", values.toString());
    }
}
