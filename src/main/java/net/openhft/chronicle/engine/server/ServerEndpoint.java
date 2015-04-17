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
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChannelProvider;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapWireHandler;
import net.openhft.chronicle.map.Replica;
import net.openhft.chronicle.network.AcceptorEventHandler;
import net.openhft.chronicle.network.WireHandler;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.network.event.WireHandlers;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerEndpoint.class);

    private EventGroup eg = new EventGroup();
    private ReplicationHub replicationHub;
    private byte localIdentifier;
    private Map<Integer, Replica> channelMap;
    private AcceptorEventHandler eah;
    private WireHandler mapWireHandler;
    private WireHandler queueWireHandler;
    private final ChannelProvider provider;

    public ServerEndpoint(byte localIdentifier) throws IOException {

        this.localIdentifier = localIdentifier;
        final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                .of(8085)
                .heartBeatInterval(1, SECONDS);

        replicationHub = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                .createWithId(localIdentifier);

        // this is how you add maps after the custer is created
        of(byte[].class, byte[].class)
                .instance().replicatedViaChannel(replicationHub.createChannel((short) 1)).create();

        provider = ChannelProvider.getProvider(replicationHub);
        this.channelMap = provider.chronicleChannelMap();

        start(0);

    }

    public AcceptorEventHandler start(int port) throws IOException {
        eg.start();

        AcceptorEventHandler eah = new AcceptorEventHandler(port, () -> {

            final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<byte[], byte[]>>> mapFactory
                    = () -> of(byte[].class, byte[].class).instance();
            final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<String, Integer>>> channelNameToIdFactory = () -> of(String.class, Integer.class).instance();

            final Map<Long, CharSequence> cidToCsp = new HashMap<>();

            mapWireHandler = new MapWireHandler<>(
                    mapFactory,
                    channelNameToIdFactory,
                    replicationHub,
                    localIdentifier,
                    channelMap,
                    cidToCsp);

            try {
                // todo move andimprove this so that it uses a chronicle based on the CSP name,
                // todo this code
                final File file = File.createTempFile("chron", "q");
                queueWireHandler = new QueueWireHandler();
            } catch (IOException e) {
                LOG.error("", e);
                queueWireHandler = null;
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
        provider.close();
    }
}
