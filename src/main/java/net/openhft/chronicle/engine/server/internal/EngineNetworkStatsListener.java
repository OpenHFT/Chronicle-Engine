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
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.network.MarshallableFunction;
import net.openhft.chronicle.network.NetworkStats;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * @author Rob Austin.
 */
public class EngineNetworkStatsListener implements NetworkStatsListener<EngineWireNetworkContext> {

    private final Asset asset;
    private final int localIdentifier;
    private final ThreadLocal<WireNetworkStats> wireNetworkStats;
    private QueueView qv;

    public EngineNetworkStatsListener(Asset asset, int localIdentifier) {
        this.localIdentifier = localIdentifier;
        wireNetworkStats = ThreadLocal.withInitial(() -> new WireNetworkStats(localIdentifier));
        this.asset = asset;
    }

    private QueueView acquireQV() {
        if (qv != null)
            return qv;
        String path = "/proc/connections/cluster/throughput/" + localIdentifier;
        RequestContext requestContext = requestContext(path)
                .elementType(NetworkStats.class);

        qv = asset.root().acquireAsset(requestContext
                .fullName()).acquireView(QueueView.class, requestContext);
        return qv;
    }

    @Override
    public void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond, @NotNull EngineWireNetworkContext nc) {
        final WireNetworkStats wireNetworkStats = this.wireNetworkStats.get();
        wireNetworkStats.writeBps(writeBps);
        wireNetworkStats.readBps(readBps);
        wireNetworkStats.socketPollCountPerSecond(socketPollCountPerSecond);
        wireNetworkStats.timestamp(System.currentTimeMillis());

        final SessionDetailsProvider sessionDetailsProvider = nc.sessionDetails();
        if (sessionDetailsProvider != null) {
            wireNetworkStats.clientId(sessionDetailsProvider.clientId());
            wireNetworkStats.userId(sessionDetailsProvider.userId());
        }

        wireNetworkStats.timestamp(System.currentTimeMillis());
        acquireQV().publisher(wireNetworkStats);

    }

    @Override
    public void onHostPort(String hostName, int port) {
        final WireNetworkStats wireNetworkStats = this.wireNetworkStats.get();
        wireNetworkStats.host(hostName);
        wireNetworkStats.port(port);
    }

    public static class Factory implements
            MarshallableFunction<ClusterContext,
                    NetworkStatsListener>, Demarshallable {

        @UsedViaReflection
        private Factory(@NotNull WireIn wireIn) {
        }

        public Factory() {
        }

        @Override
        public NetworkStatsListener apply(ClusterContext context) {
            return new EngineNetworkStatsListener(((EngineClusterContext) context).assetRoot(),
                    context.localIdentifier());
        }
    }
}

