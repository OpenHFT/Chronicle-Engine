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

package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.client.RemoteTcpClientChronicleContext;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.server.ServerEndpoint;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * @author Rob Austin.
 */
public class WireRemoteStatelessQueueClientTest extends ThreadMonitoringTest {

    /*private static final Logger LOG = LoggerFactory.getLogger(WireRemoteStatelessQueueClientTest.class);

    @Test(timeout = 50000)
    @Ignore
    public void testLastWrittenIndex() throws IOException, InterruptedException {

        try (RemoteQueueSupplier remoteQueueSupplier = new RemoteQueueSupplier()) {
            final ChronicleQueue clientQueue = remoteQueueSupplier.get();
            //Create an appender
            ExcerptAppender appender = clientQueue.createAppender();
            StringBuilder sb = new StringBuilder();
            ExcerptTailer tailer = clientQueue.createTailer();
            long lastIndex = -1;

            for (int i = 0; i < 5; i++) {
                final int finalI = i;
                appender.writeDocument(wire -> wire.write(() -> "Message").text("Hello" + finalI));

//                System.out.println(Wires.fromSizePrefixedBlobs(tailer.wire().bytes()));
                assertTrue(tailer.readDocument(wire -> {
                    wire.read(() -> "Message")
                            .text(sb);
                    assertEquals("Hello" + finalI, sb.toString());
                }));

                System.out.println(lastIndex = appender.lastWrittenIndex());
            }

            assertEquals(lastIndex, clientQueue.lastWrittenIndex());


            System.out.println("Result: " + sb);
        }
    }


    public static class RemoteQueueSupplier implements Closeable, Supplier<ChronicleQueue> {

        private final ServerEndpoint serverEndpoint;
        private final ChronicleQueue queue;
        private final RemoteTcpClientChronicleContext context;

        public RemoteQueueSupplier() throws IOException {
            serverEndpoint = new ServerEndpoint((byte) 1, new ChronicleEngine());
            int serverPort = serverEndpoint.getPort();

            context = new RemoteTcpClientChronicleContext("localhost", serverPort, (byte) 2);
            queue = context.getQueue("test");
        }


        @Override
        public void close() throws IOException {
            if (queue != null)
                queue.close();
            context.close();
            serverEndpoint.close();
        }


        @Override
        public ChronicleQueue get() {
            return queue;
        }
    }*/


}




