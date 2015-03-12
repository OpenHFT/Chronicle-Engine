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
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RemoteTcpClientTest {

    @Test
    public void testProcess() throws Exception {

        // sever
        final ServerEndpoint serverEndpoint = new ServerEndpoint((byte) 1);

        //client
        final RemoteClientServiceLocator remoteClientServiceLocator = new RemoteClientServiceLocator("localhost", serverEndpoint.getPort(), (byte) 2);
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

        {
            ChronicleMap<String, Long> numbers = context.getMap("Numbers", String.class, Long.class);
            numbers.put("Rob", 123L);
            numbers.put("Peter", 101010101L);

            assertEquals(2, numbers.size());
            assertEquals(Long.valueOf(123L), numbers.get("Rob"));
            assertEquals(Long.valueOf(101010101L), numbers.get("Peter"));
        }

        // test using Marshallable Keys
        {
            {
                ChronicleMap<MyMarshallable, Long> numbers = context.getMap("MarshallableKeys", MyMarshallable.class, Long.class);
                MyMarshallable key1 = new MyMarshallable("key1");
                MyMarshallable key2 = new MyMarshallable("key2");
                numbers.put(key1, 1L);
                numbers.put(key2, 2L);
            }
            {
                ChronicleMap<MyMarshallable, Long> numbers = context.getMap("MarshallableKeys", MyMarshallable.class, Long.class);
                MyMarshallable key1 = new MyMarshallable("key1");
                MyMarshallable key2 = new MyMarshallable("key2");
                assertEquals(2, numbers.size());
                assertEquals(Long.valueOf(1), numbers.get(key1));
                assertEquals(Long.valueOf(2), numbers.get(key2));
            }
        }

        serverEndpoint.stop();
    }


    class MyMarshallable implements Marshallable {

        String someData;

        public MyMarshallable(String someData) {
            this.someData = someData;
        }

        @Override
        public void writeMarshallable(WireOut wire) {
            wire.write(() -> "MyField").text(someData);
        }

        @Override
        public void readMarshallable(WireIn wire) throws IllegalStateException {
            someData = wire.read(() -> "MyField").text();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyMarshallable that = (MyMarshallable) o;

            if (someData != null ? !someData.equals(that.someData) : that.someData != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return someData != null ? someData.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MyMarshable{" + "someData='" + someData + '\'' + '}';
        }
    }


}