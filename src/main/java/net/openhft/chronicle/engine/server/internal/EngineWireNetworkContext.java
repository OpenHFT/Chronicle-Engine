/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZonedDateTime;

import static net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext.ConnectionStatus.CONNECTED;
import static net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext.ConnectionStatus.DISCONNECTED;

/**
 * @author Rob Austin.
 */
public class EngineWireNetworkContext<T extends EngineWireNetworkContext>
        extends VanillaNetworkContext<T> {

    static final Logger LOG = LoggerFactory.getLogger(EngineWireNetworkContext.class);
    private static final String PROC_CONNECTIONS_CLUSTER = "/proc/connections/cluster/";
    private static final String CONNECTIVITY_URI = PROC_CONNECTIONS_CLUSTER + "connectivity";
    private static final String CONNECTIVITY_HOSTS_URI = PROC_CONNECTIONS_CLUSTER + "hosts";

    private Asset rootAsset;
    private MapView<ConnectionDetails, ConnectionEvent> hostByConnectionStatus;
    private MapView<String, ConnectionStatus> connectivityHosts;
    private TcpHandler handler;

    public TcpHandler handler() {
        return handler;
    }

    public EngineWireNetworkContext(Asset asset) {
        this.rootAsset = asset.root();
        // TODO make configurable
        serverThreadingStrategy(ServerThreadingStrategy.CONCURRENT);
        ((VanillaAsset) rootAsset.acquireAsset("/proc")).configMapServer();

        hostByConnectionStatus = rootAsset.root().acquireMap(
                CONNECTIVITY_URI,
                ConnectionDetails.class,
                ConnectionEvent.class);

        connectivityHosts = rootAsset.root().acquireMap(
                CONNECTIVITY_HOSTS_URI,
                String.class,
                ConnectionStatus.class);
    }

    @NotNull
    public Asset rootAsset() {
        return this.rootAsset;
    }

    @Override
    public void onHandlerChanged(TcpHandler handler) {
        this.handler = handler;
    }

    @Override
    public ConnectionListener acquireConnectionListener() {

        return new ConnectionListener() {

            @Override
            public void onConnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
                ConnectionDetails key = new ConnectionDetails(localIdentifier, remoteIdentifier, isAcceptor);
                hostByConnectionStatus.put(key, new ConnectionEvent(CONNECTED));
                LOG.info(key + ", connectionStatus=" + CONNECTED);

                onConnectionChanged(localIdentifier, remoteIdentifier, isAcceptor);
            }

            @Override
            public void onDisconnected(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
                ConnectionDetails key = new ConnectionDetails(localIdentifier, remoteIdentifier, isAcceptor);
                hostByConnectionStatus.put(key, new ConnectionEvent(DISCONNECTED));
                LOG.info(key + ", connectionStatus=" + DISCONNECTED);
                onConnectionChanged(localIdentifier, remoteIdentifier, isAcceptor);

            }

            private void onConnectionChanged(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
                ConnectionDetails k1a = new ConnectionDetails(localIdentifier,
                        remoteIdentifier, isAcceptor);
                ConnectionDetails k1b = new ConnectionDetails(remoteIdentifier,
                        localIdentifier, !isAcceptor);

                ConnectionDetails k2a = new ConnectionDetails(localIdentifier,
                        remoteIdentifier, !isAcceptor);
                ConnectionDetails k2b = new ConnectionDetails(remoteIdentifier,
                        localIdentifier, isAcceptor);

                ConnectionStatus connectionStatus = DISCONNECTED;
                if ((get(k1a) == CONNECTED && get(k1b) == CONNECTED) ||
                        (get(k2a) == CONNECTED && get(k2b) == CONNECTED)) {
                    connectionStatus = CONNECTED;
                }

                if (localIdentifier < remoteIdentifier)
                    connectivityHosts.put("" + localIdentifier + "<->" + remoteIdentifier,
                            connectionStatus);
                else
                    connectivityHosts.put("" + remoteIdentifier + "<->" + localIdentifier,
                            connectionStatus);
            }

            private ConnectionStatus get(ConnectionDetails connectionDetails) {
                ConnectionEvent connectionEvent = hostByConnectionStatus.get(connectionDetails);
                if (connectionEvent == null)
                    return DISCONNECTED;
                return connectionEvent.connectionStatus;
            }


        };

    }

    @Override
    public String toString() {
        return "hostByConnectionStatus=" + hostByConnectionStatus.entrySet().toString();
    }

    enum ConnectionStatus implements Serializable {
        CONNECTED, DISCONNECTED
    }

    private static class ConnectionEvent extends AbstractMarshallable implements Serializable {

        // has to be a string as enums wont go in the chronicle map
        public ConnectionStatus connectionStatus;

        public ZonedDateTime timeStamp;

        ConnectionEvent(ConnectionStatus connectionStatus) {
            this.connectionStatus = connectionStatus;
            this.timeStamp = ZonedDateTime.now();
        }

    }

    public static class ConnectionDetails extends AbstractMarshallable implements Serializable {
        public int localIdentifier;
        public int remoteIdentifier;
        public boolean isAcceptor;

        ConnectionDetails(int localIdentifier, int remoteIdentifier, boolean isAcceptor) {
            this.localIdentifier = localIdentifier;
            this.remoteIdentifier = remoteIdentifier;
            this.isAcceptor = isAcceptor;
        }

        public int localIdentifier() {
            return localIdentifier;
        }

        public int remoteIdentifier() {
            return remoteIdentifier;
        }

        @Override
        public String toString() {
            return "localId=" + localIdentifier + ", remoteId=" + remoteIdentifier + ", isAcceptor=" + isAcceptor;
        }
    }

    public static class Factory implements
            MarshallableFunction<ClusterContext,
                    NetworkContext>, Demarshallable {

        @UsedViaReflection
        private Factory(@NotNull WireIn wireIn) {
        }

        public Factory() {
        }

        @Override
        public NetworkContext apply(ClusterContext context) {
            return new EngineWireNetworkContext<>(((EngineClusterContext) context).assetRoot());
        }
    }
}

