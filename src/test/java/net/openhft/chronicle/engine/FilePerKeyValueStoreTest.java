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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.tree.LeafViewFactory;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.map.VanillaStringStringKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.core.Jvm.pause;
import static net.openhft.chronicle.engine.Chassis.*;
import static org.junit.Assert.assertEquals;

/**
 * JUnit test class to support
 */
public class FilePerKeyValueStoreTest {
    public static final String NAME = "fileperkvstoretests";
    private Map<String, String> map;

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
    public void createMap() throws IOException {
        resetChassis();
        WireType writeType = WireType.TEXT;
        ((VanillaAsset) assetTree().root()).enableTranslatingValuesToBytesStore();

        LeafViewFactory<AuthenticatedKeyValueStore> factory = (context, asset) -> new FilePerKeyValueStore(context.basePath(OS.TARGET).wireType(writeType), asset);
        assetTree().root().addLeafRule(AuthenticatedKeyValueStore.class, "FilePer Key", factory);

        map = acquireMap(NAME, String.class, String.class);
        KeyValueStore mapU = ((VanillaMapView) map).underlying();
        assertEquals(VanillaStringStringKeyValueStore.class, mapU.getClass());
        assertEquals(FilePerKeyValueStore.class, mapU.underlying().getClass());

        //just in case it hasn't been cleared up last time
        map.clear();
        // allow the events to be picked up.
        pause(50);
    }

    @After
    public void cleanUp() {
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
