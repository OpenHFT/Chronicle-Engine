package net.openhft.chronicle.engine2;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.MapEvent;
import net.openhft.chronicle.engine2.map.InsertedEvent;
import net.openhft.chronicle.engine2.map.RemovedEvent;
import net.openhft.chronicle.engine2.map.UpdatedEvent;
import net.openhft.chronicle.engine2.session.LocalSession;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static net.openhft.chronicle.engine2.Chassis.*;
import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;
import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * Created by peter on 22/05/15.
 */
public class ChassisTest {
    @Before
    public void setUp() {
        resetChassis();
    }

    @Test
    public void simpleGetMapView() {
        ConcurrentMap<String, String> map = acquireMap("map-name", String.class, String.class);

        registerTopicSubscriber("map-name", String.class, String.class, (t, e) -> System.out.println("{ key: " + t + ", event: " + e + " }"));

        map.put("Hello", "World");

        ConcurrentMap<String, String> map2 = acquireMap("map-name", String.class, String.class);
        assertSame(map, map2);

        map2.put("Bye", "soon");

        map2.put("Bye", "now.");
    }

    @Test
    @Ignore
    public void subscription() {
        ConcurrentMap<String, String> map = acquireMap("map-name?putReturnsNull=true", String.class, String.class);

        map.put("Key-1", "Value-1");
        map.put("Key-2", "Value-2");

        assertEquals(2, map.size());

        // test the bootstrap finds old keys
        Subscriber<String> subscriber = createMock(Subscriber.class);
        subscriber.onMessage("Key-1");
        subscriber.onMessage("Key-2");
        replay(subscriber);
        registerSubscriber("map-name?bootstrap=true", String.class, subscriber);
        verify(subscriber);
        reset(subscriber);

        assertEquals(2, map.size());

        // test the topic publish triggers events
        subscriber.onMessage("Topic-1");
        replay(subscriber);

        TopicPublisher<String, String> publisher = acquireTopicPublisher("map-name", String.class, String.class);
        publisher.publish("Topic-1", "Message-1");
        verify(subscriber);
        reset(subscriber);
        assertEquals(3, map.size());

        subscriber.onMessage("Hello");
        subscriber.onMessage("Bye");
        subscriber.onMessage("Key-1");
        replay(subscriber);

        // test plain puts trigger events
        map.put("Hello", "World");
        map.put("Bye", "soon");
        map.remove("Key-1");
        verify(subscriber);

        assertEquals(4, map.size());

        // check the contents.
        assertEquals("Topic-1=Message-1\n" +
                        "Key-2=Value-2\n" +
                        "Hello=World\n" +
                        "Bye=soon",
                map.entrySet().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("\n")));

        assertEquals("Topic-1, Key-2, Hello, Bye",
                map.keySet().stream()
                        .collect(Collectors.joining(", ")));

        assertEquals("Message-1, Value-2, World, soon",
                map.values().stream()
                        .collect(Collectors.joining(", ")));
    }

    @Test
    @Ignore
    public void keySubscription() {
        ConcurrentMap<String, String> map = acquireMap("map-name?putReturnsNull=true", String.class, String.class);

        map.put("Key-1", "Value-1");
        map.put("Key-2", "Value-2");

        assertEquals(2, map.size());

        // test the bootstrap finds the old value
        Subscriber<String> subscriber = createMock(Subscriber.class);
        subscriber.onMessage("Value-1");
        replay(subscriber);
        registerSubscriber("map-name/Key-1?bootstrap=true", String.class, subscriber);
        verify(subscriber);
        reset(subscriber);

        assertEquals(2, map.size());

        // test the topic publish triggers events
        subscriber.onMessage("Message-1");
        replay(subscriber);

        TopicPublisher<String, String> publisher = acquireTopicPublisher("map-name", String.class, String.class);
        publisher.publish("Key-1", "Message-1");
        publisher.publish("Key-2", "Message-2");
        verify(subscriber);
        reset(subscriber);

        subscriber.onMessage("Bye");
        subscriber.onMessage(null);
        replay(subscriber);

        // test plain puts trigger events
        map.put("Key-1", "Bye");
        map.put("Key-3", "Another");
        map.remove("Key-1");
        verify(subscriber);
    }

    @Test
    @Ignore
    public void topicSubscription() {
        ConcurrentMap<String, String> map = acquireMap("map-name?putReturnsNull=true", String.class, String.class);

        map.put("Key-1", "Value-1");
        map.put("Key-2", "Value-2");

        assertEquals(2, map.size());

        // test the bootstrap finds old keys
        TopicSubscriber<String, String> subscriber = createMock(TopicSubscriber.class);
        subscriber.onMessage("Key-1", "Value-1");
        subscriber.onMessage("Key-2", "Value-2");
        replay(subscriber);
        registerTopicSubscriber("map-name?bootstrap=true", String.class, String.class, subscriber);
        verify(subscriber);
        reset(subscriber);

        assertEquals(2, map.size());

        // test the topic publish triggers events
        subscriber.onMessage("Topic-1", "Message-1");
        replay(subscriber);

        TopicPublisher<String, String> publisher = acquireTopicPublisher("map-name", String.class, String.class);
        publisher.publish("Topic-1", "Message-1");
        verify(subscriber);
        reset(subscriber);
        assertEquals(3, map.size());

        subscriber.onMessage("Hello", "World");
        subscriber.onMessage("Bye", "soon");
        subscriber.onMessage("Key-1", null);
        replay(subscriber);

        // test plain puts trigger events
        map.put("Hello", "World");
        map.put("Bye", "soon");
        map.remove("Key-1");
        verify(subscriber);

        assertEquals(4, map.size());

        // check the contents.
        assertEquals("Topic-1=Message-1\n" +
                        "Key-2=Value-2\n" +
                        "Hello=World\n" +
                        "Bye=soon",
                map.entrySet().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("\n")));

        assertEquals("Topic-1, Key-2, Hello, Bye",
                map.keySet().stream()
                        .collect(Collectors.joining(", ")));

        assertEquals("Message-1, Value-2, World, soon",
                map.values().stream()
                        .collect(Collectors.joining(", ")));
    }

    @Test
    public void entrySubscription() {
        ConcurrentMap<String, String> map = acquireMap("map-name?putReturnsNull=true", String.class, String.class);

        map.put("Key-1", "Value-1");
        map.put("Key-2", "Value-2");

        assertEquals(2, map.size());

        // test the bootstrap finds old keys
        Subscriber<MapEvent<String, String>> subscriber = createMock(Subscriber.class);
        subscriber.onMessage(InsertedEvent.of("Key-1", "Value-1"));
        subscriber.onMessage(InsertedEvent.of("Key-2", "Value-2"));
        replay(subscriber);
        registerSubscriber("map-name?bootstrap=true", MapEvent.class, (Subscriber) subscriber);
        verify(subscriber);
        reset(subscriber);

        assertEquals(2, map.size());

        // test the topic publish triggers events
        subscriber.onMessage(UpdatedEvent.of("Key-1", "Value-1", "Message-1"));
        subscriber.onMessage(InsertedEvent.of("Topic-1", "Message-1"));
        replay(subscriber);

        TopicPublisher<String, String> publisher = acquireTopicPublisher("map-name", String.class, String.class);
        publisher.publish("Key-1", "Message-1");
        publisher.publish("Topic-1", "Message-1");
        verify(subscriber);
        reset(subscriber);
        assertEquals(3, map.size());

        subscriber.onMessage(InsertedEvent.of("Hello", "World"));
        subscriber.onMessage(InsertedEvent.of("Bye", "soon"));
        subscriber.onMessage(RemovedEvent.of("Key-1", "Message-1"));
        replay(subscriber);

        // test plain puts trigger events
        map.put("Hello", "World");
        map.put("Bye", "soon");
        map.remove("Key-1");
        verify(subscriber);

        assertEquals(4, map.size());
    }

    @Test
    public void newNode() {
        Asset group = acquireAsset("group", Void.class, null, null);
        Asset subgroup = acquireAsset("group/sub-group?option=unknown", Void.class, null, null);
        assertEquals("group/sub-group", subgroup.fullName());

        Asset group2 = acquireAsset("group2/sub-group?who=knows", Void.class, null, null);
        assertEquals("group2/sub-group", group2.fullName());
    }

    @Test(expected = AssetNotFoundException.class)
    public void noAsset() {
        registerTopicSubscriber("map-name", String.class, String.class, (t, e) -> System.out.println("{ key: " + t + ", event: " + e + " }"));
    }

    @Test(expected = AssetNotFoundException.class)
    public void noInterceptor() {
        Asset asset = acquireAsset("", null, null, null);

        asset.acquireView(requestContext("").viewType(MyInterceptor.class));
    }

    @Test
    @Ignore
    public void generateInterceptor() {
        Asset asset = acquireAsset("", null, null, null);

        asset.registerFactory(MyInterceptor.class, (context, asset2, underlyingSupplier) -> {
            assertEquals(MyInterceptor.class, context.type());
            return new MyInterceptor();
        });
        MyInterceptor mi = asset.acquireView(requestContext("").viewType(MyInterceptor.class));
        MyInterceptor mi2 = asset.acquireView(requestContext("").viewType(MyInterceptor.class));
        assertNotNull(mi);
        assertSame(mi, mi2);
    }

    static class MyInterceptor implements View {

        @Override
        public View forSession(LocalSession session, Asset asset) {
            throw new UnsupportedOperationException("todo");
        }
    }
}
