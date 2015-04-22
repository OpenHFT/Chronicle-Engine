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
package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.engine.client.internal.QueueWireHandler;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapWireConnectionHub;
import net.openhft.chronicle.map.MapWireHandler;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.WireHandler;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.network.event.WireHandlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerEndpoint.class);
    private final byte localIdentifier;


    private EventGroup eg = new EventGroup();

    private AcceptorEventHandler eah;
    private WireHandler mapWireHandler;
    private WireHandler queueWireHandler;
    // private final ChannelProvider provider;


    MapWireConnectionHub mapWireConnectionHub;


    public ServerEndpoint(byte localIdentifier) throws IOException {
        this.localIdentifier = localIdentifier;
        start(0);

    }

    public MapWireConnectionHub mapWireConnectionHub() {
        return mapWireConnectionHub;
    }

    public AcceptorEventHandler start(int port) throws IOException {
        eg.start();

        AcceptorEventHandler eah = new AcceptorEventHandler(port, () -> {

            final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<byte[], byte[]>>> mapFactory
                    = () -> of(byte[].class, byte[].class).instance();
            final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<String, Integer>>> channelNameToIdFactory = () -> of(String.class, Integer.class).instance();

            final Map<Long, CharSequence> cidToCsp = new HashMap<>();

            try {
                // todo move andimprove this so that it uses a chronicle based on the CSP name,
                // todo this code
                final File file = File.createTempFile("chron", "q");
                queueWireHandler = new QueueWireHandler();
            } catch (IOException e) {
                LOG.error("", e);
                queueWireHandler = null;
            }

            try {
                mapWireConnectionHub = new MapWireConnectionHub(mapFactory, channelNameToIdFactory, localIdentifier, 8085);
                mapWireHandler = new MapWireHandler<>(cidToCsp, mapWireConnectionHub);
            } catch (IOException e) {
                LOG.error("", e);
            }

            EngineWireHandler engineWireHandler = new EngineWireHandler(
                    mapWireHandler,
                    queueWireHandler,
                    cidToCsp);

            ((Consumer<WireHandlers>) mapWireHandler).accept(engineWireHandler);

            return engineWireHandler;
        });

        eg.addHandler(eah);
        this.eah = eah;
        return eah;


    }

    public int getPort() throws IOException {
        return eah.getLocalPort();
    }

    public void stop() {
        eg.stop();
    }

    @Override
    public void close() throws IOException {
        stop();
        eg.close();
        eah.close();
        mapWireConnectionHub.close();
    }
}
