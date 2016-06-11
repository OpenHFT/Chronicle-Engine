package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.engine.HeartbeatHandler;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.fs.EngineConnectionManager;
import net.openhft.chronicle.engine.server.internal.EngineNetworkStatsListener;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.engine.server.internal.UberHandler;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.HostIdConnectionStrategy;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class EngineClusterContext extends ClusterContext {
    private static final Logger LOG = LoggerFactory.getLogger(EngineClusterContext.class);
    Asset assetRoot;
    private byte localIdentifier;

    @UsedViaReflection
    private EngineClusterContext(WireIn w) {
        super(w);
    }

    public EngineClusterContext() {
        super();
    }


    public ThrowingFunction<NetworkContext, TcpEventHandler, IOException> tcpEventHandlerFactory() {
        return (networkContext) -> {
            final EngineWireNetworkContext nc = (EngineWireNetworkContext) networkContext;

            if (nc.isAcceptor())
                nc.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));
            final TcpEventHandler handler = new TcpEventHandler(networkContext);

            final Function<Object, TcpHandler> consumer = o -> {
                if (o instanceof SessionDetailsProvider) {
                    final SessionDetailsProvider sessionDetails = (SessionDetailsProvider) o;
                    nc.heartbeatTimeoutMs(heartbeatTimeoutMs());
                    nc.sessionDetails(sessionDetails);
                    nc.wireType(sessionDetails.wireType());

                    final WireType wireType = nc.sessionDetails().wireType();
                    if (wireType != null)
                        nc.wireOutPublisher().wireType(wireType);
                    return new EngineWireHandler();
                } else if (o instanceof TcpHandler)
                    return (TcpHandler) o;

                throw new UnsupportedOperationException("not supported class=" + o.getClass());
            };


            // todo log these to a chronicle q rather than the log
            nc.networkStatsListener(new NetworkStatsListener() {

                String host;
                long port;

                @Override
                public void onNetworkStats(long writeBps, long readBps, long socketPollCountPerSecond, @NotNull NetworkContext networkContext) {
                    LOG.info("writeBps=" + writeBps + ", readBps=" + readBps +
                            ", socketPollCountPerSecond=" + socketPollCountPerSecond +
                            ", host=" + host + ", port=" + port);
                }

                @Override
                public void onHostPort(String hostName, int port) {
                    host = hostName;
                    this.port = port;
                }
            });

            final Function<EngineWireNetworkContext, TcpHandler> f
                    = x -> new HeaderTcpHandler<>(handler, consumer, x);

            final WireTypeSniffingTcpHandler sniffer = new
                    WireTypeSniffingTcpHandler<>(handler, nc, f);

            handler.tcpHandler(sniffer);
            return handler;

        };
    }

    public Asset assetRoot() {
        return assetRoot;
    }

    public EngineClusterContext assetRoot(Asset assetRoot) {
        this.assetRoot = assetRoot;
        localIdentifier = HostIdentifier.localIdentifier(assetRoot);
        localIdentifier(localIdentifier);
        eventLoop(assetRoot.findOrCreateView(EventLoop.class));
        return this;
    }

    @Override
    public void defaults() {
        wireType(WireType.TEXT);
        handlerFactory(new UberHandler.Factory());
        wireOutPublisherFactory(new VanillaWireOutPublisherFactory());
        networkContextFactory(new EngineWireNetworkContext.Factory());
        networkStatsListenerFactory(new EngineNetworkStatsListener.Factory());
        connectionEventHandler(new EngineConnectionManager.Factory());
        heartbeatFactory(new HeartbeatHandler.Factory());
        heartbeatTimeoutMs(5_000L);
        heartbeatIntervalMs(1_000L);
        connectionStrategy(new HostIdConnectionStrategy());
    }
}
