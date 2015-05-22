package net.openhft.chronicle.engine2;

import net.openhft.chronicle.engine2.map.VanillaKeyValueStore;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by peter on 22/05/15.
 */
public class EngineTest {
    @Test//(expected = UnsupportedOperationException.class)
    public void simpleGetMapView() {
        CommonSession.add("map-name", new VanillaKeyValueStore<>());

        CommonSession.register("map-name", String.class, (t, e) -> System.out.println("key: " + t + " event: " + e));

        ConcurrentMap<String, String> map = CommonSession.acquireMap("map-name", String.class, String.class);

        map.put("Hello", "World");
    }
}
