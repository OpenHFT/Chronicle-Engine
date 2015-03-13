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

import net.openhft.chronicle.engine.client.RemoteTcpClientChronicleContext;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.map.ChronicleMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;


/**
 * @author Rob Austin.
 */
public class WireRemoteStatelessClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(WireRemoteStatelessClientTest.class);

    @Test(timeout = 10000)
    public void testPutAndGet() throws IOException, InterruptedException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class, CharSequence.class)) {

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());
        }
    }


   public static class RemoteMapSupplier<K, V> implements Closeable, Supplier<ChronicleMap<K, V>> {

        private final ServerEndpoint serverEndpoint;
        private final ChronicleMap<K, V> map;

        public RemoteMapSupplier(Class<K> kClass, Class<V> vClass) throws IOException {

            serverEndpoint = new ServerEndpoint((byte) 1);
            int serverPort = serverEndpoint.getPort();

            map = new RemoteTcpClientChronicleContext(
                    "localhost", serverPort).getMap("test", kClass, vClass);
        }


        @Override
        public void close() throws IOException {
            if (map != null)
                map.close();

            serverEndpoint.close();
        }


        @Override
        public ChronicleMap<K, V> get() {
            return map;
        }
    }


}




