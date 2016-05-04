/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.MarshallableFunction;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.VanillaNetworkContext;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * @author Rob Austin.
 */
public class EngineWireNetworkContext<T extends EngineWireNetworkContext> extends VanillaNetworkContext<T> {

    private Asset rootAsset;

    public EngineWireNetworkContext(Asset asset) {
        this.rootAsset = asset.root();
    }

    @NotNull
    public Asset rootAsset() {
        return rootAsset;
    }

    @Override
    public String toString() {

        byte localId = -1;
        if (rootAsset == null) {
            return "";
        }

        final Asset root = rootAsset;
        final HostIdentifier hostIdentifier = root.getView(HostIdentifier.class);

        if (hostIdentifier != null)
            localId = hostIdentifier.hostId();

        return "EngineWireNetworkContext{" +
                "localId=" + localId +
                "rootAsset=" + root +
                '}';
    }


    public static class ConnectionDetails extends AbstractMarshallable {
        int localIdentifier;
        int remoteIdentifier;

        public ConnectionDetails(int localIdentifier, int remoteIdentifier) {
            this.localIdentifier = localIdentifier;
            this.remoteIdentifier = remoteIdentifier;
        }
    }

    public enum ConnectionStatus {
        CONNECTED, DISCONNECTED
    }

    Map<ConnectionDetails, ConnectionStatus> mapView;


    @Override
    public ConnectionListener acquireConnectionListener() {

        return new ConnectionListener() {

            @Override
            public void onConnected(int localIdentifier, int remoteIdentifier) {
                acquireMap().put(new ConnectionDetails(localIdentifier, remoteIdentifier), ConnectionStatus.CONNECTED);
            }

            // we have to do this because of the build order of the mapview
            Map<ConnectionDetails, ConnectionStatus> acquireMap() {
                if (mapView == null)
                    mapView = rootAsset().root()
                            .acquireView(MapView.class, RequestContext.requestContext
                                    ("/proc/connections").type(EngineWireNetworkContext
                                    .ConnectionDetails.class).type2(EngineWireNetworkContext.ConnectionStatus.class));
                return mapView;
            }

            @Override
            public void onDisconnected(int localIdentifier, int remoteIdentifier) {
                acquireMap().put(new ConnectionDetails(localIdentifier, remoteIdentifier), ConnectionStatus.DISCONNECTED);
            }
        };

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
        public void writeMarshallable(@NotNull WireOut wire) {

        }

        @Override
        public NetworkContext apply(ClusterContext context) {
            return new EngineWireNetworkContext<>(((EngineClusterContext) context).assetRoot());
        }
    }
}

