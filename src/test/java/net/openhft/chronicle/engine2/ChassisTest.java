package net.openhft.chronicle.engine2;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.AssetNotFoundException;
import net.openhft.chronicle.engine2.api.Interceptor;
import net.openhft.chronicle.engine2.api.map.InterceptorFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static net.openhft.chronicle.engine2.Chassis.*;
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

        registerTopicSubscriber("map-name", String.class, (t, e) -> System.out.println("{ key: " + t + ", event: " + e + " }"));

        map.put("Hello", "World");

        ConcurrentMap<String, String> map2 = acquireMap("map-name", String.class, String.class);
        assertSame(map, map2);

        map2.put("Bye", "soon");

        map2.put("Bye", "now.");
    }

    @Test
    public void keySet_entrySet() {
        ConcurrentMap<String, String> map = acquireMap("map-name", String.class, String.class);
        map.put("Hello", "World");
        map.put("Bye", "soon");

        assertEquals("Hello=World\n" +
                        "Bye=soon",
                map.entrySet().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("\n")));

        assertEquals("Hello, Bye",
                map.keySet().stream()
                        .collect(Collectors.joining(", ")));

        assertEquals("World, soon",
                map.values().stream()
                        .collect(Collectors.joining(", ")));

    }

    @Test(expected = AssetNotFoundException.class)
    public void noAsset() {
        registerTopicSubscriber("map-name", String.class, (t, e) -> System.out.println("{ key: " + t + ", event: " + e + " }"));
    }

    @Test(expected = AssetNotFoundException.class)
    public void noInterceptor() {
        Asset asset;
        try {
            asset = Chassis.acquireAsset("", null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        asset.acquireInterceptor(MyInterceptor.class);
    }

    @Test
    public void generateInterceptor() {
        Asset asset;
        try {
            asset = Chassis.acquireAsset("", null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        asset.registerInterceptor(InterceptorFactory.class, new InterceptorFactory() {
            @Override
            public <I extends Interceptor> I create(Class<I> iClass) {
                assertEquals(MyInterceptor.class, iClass);
                return (I) new MyInterceptor();
            }
        });
        MyInterceptor mi = asset.acquireInterceptor(MyInterceptor.class);
        MyInterceptor mi2 = asset.acquireInterceptor(MyInterceptor.class);
        assertNotNull(mi);
        assertSame(mi, mi2);
    }

    static class MyInterceptor implements Interceptor {


    }
}
