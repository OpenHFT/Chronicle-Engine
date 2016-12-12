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
import net.openhft.chronicle.core.util.Histogram;
import net.openhft.chronicle.engine.api.column.BarChartProperties;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal;
import net.openhft.chronicle.engine.api.column.VaadinChartSeries;
import net.openhft.chronicle.engine.api.column.VanillaVaadinChart;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.cfg.EngineClusterContext;
import net.openhft.chronicle.engine.fs.Clusters;
import net.openhft.chronicle.engine.fs.EngineCluster;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.network.MarshallableFunction;
import net.openhft.chronicle.network.NetworkStats;
import net.openhft.chronicle.network.NetworkStatsListener;
import net.openhft.chronicle.network.WireNetworkStats;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.engine.api.column.VaadinChartSeries.Type.SPLINE;
import static net.openhft.chronicle.engine.api.tree.RequestContext.requestContext;

/**
 * @author Rob Austin.
 */
public class EngineNetworkStatsListener implements NetworkStatsListener<EngineWireNetworkContext> {

    public static final String PROC_CONNECTIONS_CLUSTER_THROUGHPUT = "/proc/connections/cluster/throughput/";
    private final Asset asset;
    private final int localIdentifier;
    private final WireNetworkStats wireNetworkStats = new WireNetworkStats();
    private QueueView qv;
    private volatile boolean isClosed;
    private EngineWireNetworkContext nc;
    private Histogram histogram = null;


    static {
        RequestContext.loadDefaultAliases();
    }

    public EngineNetworkStatsListener(Asset asset, int localIdentifier) {
        this.localIdentifier = localIdentifier;
        wireNetworkStats.localIdentifier(localIdentifier);
        this.asset = asset;
    }

    private QueueView acquireQV() {

        if (qv != null)
            return qv;
        String path = PROC_CONNECTIONS_CLUSTER_THROUGHPUT + localIdentifier;

        RequestContext requestContext = requestContext(path)
                .elementType(NetworkStats.class);

        if (ChronicleQueueView.isQueueReplicationAvailable())
            requestContext.cluster(clusterName());

        qv = asset.root().acquireAsset(requestContext
                .fullName()).acquireView(QueueView.class, requestContext);
        return qv;
    }

    private String clusterName() {
        final Clusters view = asset.getView(Clusters.class);

        if (view == null)
            return "";
        final EngineCluster engineCluster = view.firstCluster();
        if (engineCluster == null)
            return "";
        return engineCluster.clusterName();
    }

    @Override
    public void networkContext(@NotNull EngineWireNetworkContext nc) {
        this.nc = nc;
    }

    @Override
    public void onNetworkStats(long writeBps, long readBps,
                               long socketPollCountPerSecond) {
        if (isClosed)
            return;

        wireNetworkStats.writeBps(writeBps);
        wireNetworkStats.readBps(readBps);
        wireNetworkStats.socketPollCountPerSecond(socketPollCountPerSecond);
        wireNetworkStats.timestamp(System.currentTimeMillis());
        wireNetworkStats.isConnected(!nc.isClosed());

        if (histogram != null) {
            wireNetworkStats.percentile50th((int) (histogram.percentile(0.5) / 1_000));
            wireNetworkStats.percentile90th((int) (histogram.percentile(0.9) / 1_000));
            wireNetworkStats.percentile99th((int) (histogram.percentile(0.99) / 1_000));
            wireNetworkStats.percentile99_9th((int) (histogram.percentile(0.999) / 1_000));

            histogram.reset();
        }

        publish();

    }

    private void nc(@NotNull EngineWireNetworkContext nc) {
        wireNetworkStats.isAcceptor(nc.isAcceptor());
        if (nc.handler() instanceof AbstractSubHandler) {
            final int remoteIdentifier = ((AbstractSubHandler) nc.handler()).remoteIdentifier();
            wireNetworkStats.remoteIdentifier(remoteIdentifier);

        } else if (nc.handler() instanceof UberHandler) {
            final UberHandler handler = (UberHandler) nc.handler();
            wireNetworkStats.remoteIdentifier(handler.remoteIdentifier());
            wireNetworkStats.wireType(handler.wireType());
        } else {
            wireNetworkStats.remoteIdentifier(0);
        }

        final SessionDetailsProvider sessionDetailsProvider = nc.sessionDetails();
        if (sessionDetailsProvider != null) {
            wireNetworkStats.clientId(sessionDetailsProvider.clientId());
            wireNetworkStats.userId(sessionDetailsProvider.userId());
            wireNetworkStats.wireType(sessionDetailsProvider.wireType());
        }
    }

    @Override
    public void onHostPort(String hostName, int port) {
        wireNetworkStats.remoteHostName(hostName);
        wireNetworkStats.remotePort(port);

        if (isClosed)
            return;
        publish();
    }

    @Override
    public void onRoundTripLatency(long nanosecondLatency) {
        acquireHistogram().sampleNanos(nanosecondLatency);
    }

    private Histogram acquireHistogram() {
        if (histogram != null)
            return histogram;

        histogram = new Histogram();
        createVaadinChart();
        return histogram;
    }

    private void createVaadinChart() {

        final String csp = PROC_CONNECTIONS_CLUSTER_THROUGHPUT + "replication-latency/" +
                localIdentifier
                + "-" + wireNetworkStats.remoteIdentifier();


        final VanillaVaadinChart barChart = asset.acquireView(requestContext(csp).view("Chart"));
        barChart.columnNameField("timestamp");
        VaadinChartSeries percentile50th = new VaadinChartSeries("percentile50th").type(SPLINE).yAxisLabel
                ("microseconds");
        VaadinChartSeries percentile90th = new VaadinChartSeries("percentile90th").type(SPLINE).yAxisLabel
                ("microseconds");
        VaadinChartSeries percentile99th = new VaadinChartSeries("percentile99th").type(SPLINE).yAxisLabel(
                "microseconds");
        VaadinChartSeries percentile99_9th = new VaadinChartSeries("percentile99_9th").type(SPLINE)
                .yAxisLabel("microseconds");


        barChart.series(percentile50th, percentile90th, percentile99th, percentile99_9th);

        final BarChartProperties barChartProperties = new BarChartProperties();
        barChartProperties.title = "Round Trip Network Latency Distribution";
        barChartProperties.menuLabel = "round trip latency";
        barChartProperties.countFromEnd = 100;
        barChart.barChartProperties(barChartProperties);
        barChart.dataSource(qv);
        barChartProperties.filter = new ColumnViewInternal.MarshableFilter("percentile99_9th", ">0");


    }

    @Override
    public void close() {
        if (isClosed)
            return;
        isClosed = true;
        wireNetworkStats.writeBps(0);
        wireNetworkStats.readBps(0);
        wireNetworkStats.socketPollCountPerSecond(0);
        wireNetworkStats.timestamp(System.currentTimeMillis());
        wireNetworkStats.isConnected(false);
        publish();
    }

    private void publish() {
        nc(nc);
        acquireQV().publishAndIndex("", wireNetworkStats);
    }

    @Override
    public boolean isClosed() {
        return isClosed;
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

