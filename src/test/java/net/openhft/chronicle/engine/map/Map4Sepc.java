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

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Rob Austin
 */
public class Map4Sepc {

    static ChronicleMap<Integer, String> createMap() throws IOException {
        final WireRemoteStatelessMapClientTest.RemoteMapSupplier remoteMapSupplier = new WireRemoteStatelessMapClientTest.RemoteMapSupplier(Integer.class, String.class);
        return remoteMapSupplier.get();
    }

    @Ignore
    @Test
    public void testPut() throws IOException {
        final ChronicleMap<Integer, String> map = createMap();

        Map<Integer, String> m = new HashMap<>();

        for (int i = 0; i < 5; i++) {
            m.put(i, "hello " + i);
        }

        map.putAll(m);
        map.entrySet();

        System.out.println(map.entrySet().toString());
    }
}
