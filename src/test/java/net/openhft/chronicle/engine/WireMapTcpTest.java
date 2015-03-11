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

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ServerEndpoint;
import net.openhft.chronicle.map.WiredChronicleMapStatelessClientBuilder;
import net.openhft.chronicle.map.WiredStatelessChronicleMap;
import net.openhft.chronicle.network2.AcceptorEventHandler;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;

/*
Running on an i7-3970X

TextWire: Loop back echo latency was 7.4/8.9 12/20 108/925 us for 50/90 99/99.9 99.99/worst %tile
BinaryWire: Loop back echo latency was 6.6/8.0 9/11 19/3056 us for 50/90 99/99.9 99.99/worst %tile
RawWire: Loop back echo latency was 5.9/6.8 8/10 12/80 us for 50/90 99/99.9 99.99/worst %tile
 */

public class WireMapTcpTest {




    @Test
    public void testProcess() throws Exception {


        final ServerEndpoint serverEndpoint = new ServerEndpoint((byte) 1);

        final AcceptorEventHandler eah = serverEndpoint.run();

        SocketChannel[] sc = new SocketChannel[1];
        for (int i = 0; i < sc.length; i++) {
            SocketAddress localAddress = new InetSocketAddress("localhost", eah.getLocalPort());
            System.out.println("Connecting to " + localAddress);
            sc[i] = SocketChannel.open(localAddress);
            sc[i].configureBlocking(false);
        }



        final WiredChronicleMapStatelessClientBuilder<String, ServiceLocator> builder =
                new WiredChronicleMapStatelessClientBuilder<>(
                        new InetSocketAddress("localhost", eah.getLocalPort()),
                        String.class,
                        ServiceLocator.class,
                        (short) 1);

        builder.identifier((byte)1);
        final ChronicleMap<String, ServiceLocator> detailsMap = builder.create();

        ServiceLocator serviceLocator = detailsMap.get("Colours");


        final  short colourChannelId = 2;

        // todo add some sort of cross node locking
        if (serviceLocator == null) {
            short channelID = colourChannelId;
            ((WiredStatelessChronicleMap) detailsMap).createChannel(channelID);
            serviceLocator = new ServiceLocator(String.class, String.class, channelID);
            detailsMap.put("Colours", serviceLocator);
        }

        ChronicleMap<String, String> colours = new WiredChronicleMapStatelessClientBuilder<String, String>(
                builder.hub(),
                serviceLocator.keyClass,
                serviceLocator.valueClass,
                serviceLocator.channelID).hub(builder.hub()).create();

        colours.put("Rob", "Blue");

        assertEquals(1, colours.size());


        ServiceLocator result = detailsMap.get("Colours");
        Assert.assertEquals(2, result.channelID);

        serverEndpoint.stop();
    }




    public static class ServiceLocator implements Marshallable {

        Class keyClass;
        Class valueClass;
        short channelID;


        public ServiceLocator() {
        }


        public ServiceLocator(Class<String> keyClass, Class<String> valueClass, short channelID) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.channelID = channelID;
        }


        @Override
        public void writeMarshallable(WireOut wire) {
            wire.write(() -> "keyClass").text(keyClass.getName());
            wire.write(() -> "valueClass").text(valueClass.getName());
            wire.write(() -> "channelID").int16(channelID);
        }

        @Override
        public void readMarshallable(WireIn wire) throws IllegalStateException {
            try {
                keyClass = Class.forName(wire.read(() -> "keyClass").text());
                valueClass = Class.forName(wire.read(() -> "valueClass").text());
                channelID = wire.read(() -> "channelID").int16();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
        }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ServiceLocator serviceLocator = (ServiceLocator) o;

            if (channelID != serviceLocator.channelID) return false;
            if (!keyClass.equals(serviceLocator.keyClass)) return false;
            if (!valueClass.equals(serviceLocator.valueClass)) return false;

            return true;
    }

        @Override
        public int hashCode() {
            int result = keyClass.hashCode();
            result = 31 * result + valueClass.hashCode();
            result = 31 * result + (int) channelID;
            return result;
        }
    }
}