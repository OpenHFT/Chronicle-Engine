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

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleContext;
import net.openhft.chronicle.engine.old.ChronicleEngine;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class ChronicleEngineTest {
    final ChronicleContext context = new ChronicleEngine();
    final Chronicle mockedQueue = mock(Chronicle.class);
    @SuppressWarnings("unchecked")
    final ChronicleMap<String, String> mockedMap = mock(ChronicleMap.class);
    final ChronicleSet<String> mockedSet = mock(ChronicleSet.class);

    @Before
    public void setUp() {
        ((ChronicleEngine) context).setQueue("queue1", mockedQueue);
        ((ChronicleEngine) context).setMap("map1", mockedMap);
        ((ChronicleEngine) context).setSet("set1", mockedSet);
    }

    @Test
    public void testGetMap() {
        ChronicleMap<String, String> map1 = context.getMap("map1", String.class, String.class);
        map1.put("Hello", "World");
    }

    @Test
    public void testGetSet() {
        ChronicleSet<String> set1 = context.getSet("set1", String.class);
        set1.add("Hello");
    }

    @Test
    public void testGetQueue() {
        Chronicle chronicle = context.getQueue("queue1");
        assertNotNull(chronicle);
    }
}