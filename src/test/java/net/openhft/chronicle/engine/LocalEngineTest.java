/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.FilePerKeyChronicleMap;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class LocalEngineTest {
  /*  final ChronicleContext context = new ChronicleEngine();
    final ChronicleQueue mockedQueue = mock(ChronicleQueue.class);
    final ChronicleSet<String> mockedSet = mock(ChronicleSet.class);

    @Before
    public void setUp() throws IOException {
        ((ChronicleEngine) context).setQueue("queue1", mockedQueue);
        ((ChronicleEngine) context).setSet("set1", mockedSet);
    }

    @Test
    public void testGetMap() throws IOException {
        {
            Map<String, String> map1 = context.getMap("map1", String.class, String.class);
            map1.put("Hello", "World");
        }

        {
            Map<String, String> map1 = context.getMap("map1", String.class, String.class);
            assertEquals("World", map1.get("Hello"));
        }
    }

    @Test
    public void testGetSet() {
        ChronicleSet<String> set1 = context.getSet("set1", String.class);
        set1.add("Hello");
    }

    @Test
    public void testGetQueue() {
        ChronicleQueue chronicle = context.getQueue("queue1");
        assertNotNull(chronicle);
    }*/

    @Ignore
    @Test(timeout = 120000)
    public void test2MBEntriesLocal() throws Exception {
        int factor = 5;

        ChronicleEngine engine = new ChronicleEngine();
        String dir = System.getProperty("java.io.tmpdir") + "/localtest.deleteme";
        File file = new File(dir);
        engine.setMap("localtest", new FilePerKeyChronicleMap(dir));
        try (ChronicleMap<String, String> map = engine.getMap("localtest", String.class, String.class)) {
            for (boolean test : new boolean[]{true, false}) {
                AtomicInteger count = new AtomicInteger();
                ((FilePerKeyChronicleMap) map).registerForEvents(e -> count.incrementAndGet());
                int factor0 = test ? 1 : factor;
                long time = System.currentTimeMillis();
                StringBuilder sb = new StringBuilder();
                while (sb.length() < 2 << 20)
                    sb.append(sb.length()).append('\n');

                String s = sb.toString();
                for (int f = 0; f < factor0; f++) {
                    IntStream.range(0, 50).forEach(i -> {
                        String key = "largeEntry-" + i;
                        map.put(key, s);
                        if (test)
                            assertEquals(s, map.get(key));
                    });
                }
                long rate = (System.currentTimeMillis() - time) / factor;
                for (File f : file.listFiles()) {
                    if (f.isDirectory()) continue;
                    f.delete();
                }
                TimeUnit.SECONDS.sleep(1);
                if (!test) {
                    System.out.format("factor %,d: To write 100 MB took an average of %,dms, events=%,d%n", factor,
                            rate, count.intValue());
                }
            }
        }
        file.delete();
    }
}
