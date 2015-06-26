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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.map.VanillaStringStringKeyValueStore;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static net.openhft.chronicle.core.Jvm.pause;
import static net.openhft.chronicle.engine.Chassis.*;
import static org.junit.Assert.assertEquals;

/**
 * JUnit test class to support
 */
public class FilePerKeyValueStoreTest {
    public static final String NAME = "fileperkvstoretests";
    private static Map<String, String> map;

    @BeforeClass
    public static void createMap() throws IOException {
        resetChassis();
        Function<Bytes, Wire> writeType = WireType.TEXT;
        enableTranslatingValuesToBytesStore();

        addLeafRule(AuthenticatedKeyValueStore.class, "FilePer Key",
                (context, asset) -> new FilePerKeyValueStore(context.basePath(OS.TARGET).wireType(writeType), asset));

        map = acquireMap(NAME, String.class, String.class);
        KeyValueStore mapU = ((VanillaMapView) map).underlying();
        assertEquals(VanillaStringStringKeyValueStore.class, mapU.getClass());
        assertEquals(FilePerKeyValueStore.class, mapU.underlying().getClass());

        //just in case it hasn't been cleared up last time
        map.clear();
        // allow the events to be picked up.
        pause(50);
    }

    @AfterClass
    public static void cleanUp() {
        map.clear();
    }

    @Ignore("todo fix")
    @Test
    public void test() throws InterruptedException {
        List<MapEvent<String, String>> events = new ArrayList<>();
        registerSubscriber(NAME, MapEvent.class, events::add);

        map.put("testA", "One");
        map.put("testB", "Two");
        waitFor(events, 2);
        map.put("testB", "Three");

        assertEquals(2, map.size());
        assertEquals("One", map.get("testA"));
        assertEquals("Three", map.get("testB"));

        waitFor(events, 3);
        TimeUnit.MILLISECONDS.sleep(100);
        if (events.size() != 3)
            events.forEach(System.out::println);
        assertEquals(3, events.size());
    }

    private void waitFor(@NotNull List<MapEvent<String, String>> events, int count) throws InterruptedException {
        for (int i = 1; i <= 10; i++) {
            if (events.size() >= count)
                break;
            TimeUnit.MILLISECONDS.sleep(i * i);
        }
    }
}
