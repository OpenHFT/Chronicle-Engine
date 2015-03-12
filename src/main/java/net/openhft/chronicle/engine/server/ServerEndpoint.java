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

import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.*;
import net.openhft.chronicle.network2.AcceptorEventHandler;
import net.openhft.chronicle.network2.WireHandler;
import net.openhft.chronicle.network2.event.EventGroup;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;

/**
 * Created by Rob Austin
 */
public class ServerEndpoint implements Closeable {

    private EventGroup eg = new EventGroup();
    //   private ChronicleHashInstanceBuilder<ChronicleMap> chronicleHashInstanceBuilder;
    private ReplicationHub replicationHub;
    private byte localIdentifier;
    private List<Replica> channelList;
    private AcceptorEventHandler eah;

    public ServerEndpoint(byte localIdentifier) throws IOException {

        this.localIdentifier = localIdentifier;
        final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                .of(1425)
                .heartBeatInterval(1, SECONDS);

        replicationHub = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                .createWithId(localIdentifier);

        // this is how you add maps after the custer is created
        ChronicleMap serviceLocatorMap = of(byte[].class, byte[].class)
                .instance().replicatedViaChannel(replicationHub.createChannel((short) 1)).create();

        final ChannelProvider provider = ChannelProvider.getProvider(replicationHub);

        //  chronicleHashInstanceBuilder = ;
        this.channelList = provider.chronicleChannelList();

        start();

    }

    public AcceptorEventHandler start() throws IOException {
        eg.start();

        AcceptorEventHandler eah = new AcceptorEventHandler(0, () -> {

            final WireHandler mapWireHandler = MapWireHandlerBuilder.of(
                    () -> (ChronicleHashInstanceBuilder) ChronicleMapBuilder.of(byte[].class, byte[].class).instance(),
                    replicationHub,
                    localIdentifier,
                    channelList);

            return new EngineWireHandler(
                    mapWireHandler,
                    null);
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
    }
}
