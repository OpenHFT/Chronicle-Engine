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

import net.openhft.chronicle.engine.client.RemoteTcpClientChronicleContext;
import net.openhft.chronicle.engine.client.internal.RemoteClientServiceLocator;
import net.openhft.chronicle.engine.server.internal.ServerEndpoint;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.network2.AcceptorEventHandler;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/*
Running on an i7-3970X

TextWire: Loop back echo latency was 7.4/8.9 12/20 108/925 us for 50/90 99/99.9 99.99/worst %tile
BinaryWire: Loop back echo latency was 6.6/8.0 9/11 19/3056 us for 50/90 99/99.9 99.99/worst %tile
RawWire: Loop back echo latency was 5.9/6.8 8/10 12/80 us for 50/90 99/99.9 99.99/worst %tile
 */

public class RemoteTcpClientTest {


    @Test
    public void testProcess() throws Exception {

        // sever
        final ServerEndpoint serverEndpoint = new ServerEndpoint((byte) 1);
        final AcceptorEventHandler eah = serverEndpoint.run();

        //client
        final RemoteClientServiceLocator remoteClientServiceLocator = new RemoteClientServiceLocator("localhost", eah.getLocalPort(), (byte) 2);
        final RemoteTcpClientChronicleContext context = new RemoteTcpClientChronicleContext(remoteClientServiceLocator);

        {
            final ChronicleMap<String, String> colourMap = context.getMap("Colours", String.class, String.class);
            colourMap.put("Rob", "Blue");
            colourMap.put("Peter", "Green");
            assertEquals(2, colourMap.size());

            assertEquals("Blue", colourMap.get("Rob"));
            assertEquals("Green", colourMap.get("Peter"));
        }

        {
            ChronicleMap<String, Long> numbers = context.getMap("Numbers", String.class, Long.class);
            numbers.put("Rob", 123L);
            numbers.put("Peter", 101010101L);


            assertEquals(2, numbers.size());
            assertEquals(Long.valueOf(123L), numbers.get("Rob"));
            assertEquals(Long.valueOf(101010101L), numbers.get("Peter"));
        }

        serverEndpoint.stop();
    }




}