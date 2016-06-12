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

package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.engine.ThreadMonitoringTest;

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

