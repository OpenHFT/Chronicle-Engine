package net.openhft.chronicle.engine;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class ChronicleEngineTest {
    final ChronicleEngine engine = new ChronicleEngine();
    final Chronicle mockedQueue = mock(Chronicle.class);
    @SuppressWarnings("unchecked")
    final ChronicleMap<String, String> mockedMap = mock(ChronicleMap.class);

    @Before
    public void setUp() {
        engine.setQueue("queue1", mockedQueue);
        engine.setMap("map1", mockedMap);
    }

    @Test
    public void testGetQueue() {
        ChronicleMap<String, String> map1 = engine.getMap("map1", String.class, String.class);
        map1.put("Hello", "World");
    }

    @Test
    public void testGetMap() {
        Chronicle chronicle = engine.getQueue("queue1");
        assertNotNull(chronicle);
    }
}