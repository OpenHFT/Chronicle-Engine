package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.util.ThrowingFunction;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.engine.server.internal.UberHandler;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.HeaderTcpHandler;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.TcpEventHandler;
import net.openhft.chronicle.network.WireTypeSniffingTcpHandler;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class EngineClusterContext extends ClusterContext {

    Asset assetRoot;
    private byte localIdentifier;

    private EngineClusterContext(WireIn w) {
        super(w);
    }

    public EngineClusterContext() {

    }

    private static byte localIdentifier(@NotNull Asset asset) {
        HostIdentifier hostIdentifier = asset.findOrCreateView(HostIdentifier.class);
        return hostIdentifier.hostId();
    }

    public ThrowingFunction<NetworkContext, IOException, TcpEventHandler> tcpEventHandlerFactory() {
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
        localIdentifier = localIdentifier(assetRoot);
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
        heartbeatTimeoutMs(20_000L);
    }
}
