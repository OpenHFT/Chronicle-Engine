package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.Chassis;
import net.openhft.chronicle.engine.api.set.KeySetView;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by peter.lawrey on 11/06/2015.
 */
public class MapViewTest {
    @Before
    public void setUp() {
        Chassis.resetChassis();
    }

    @Test
    public void keySet() {
        Map<String, String> map = Chassis.acquireMap("test", String.class, String.class);
        map.put("a", "one");
        map.put("b", "two");
        map.put("c", "three");
        Set<String> keys = map.keySet();
        assertTrue(KeySetView.class.isInstance(keys));
        assertEquals("[a, b, c]", keys.toString());
        assertEquals(new HashSet<>(keys), keys);
        int hc = keys.hashCode();
        assertEquals(new HashSet<>(keys).hashCode(), hc);
    }
}
