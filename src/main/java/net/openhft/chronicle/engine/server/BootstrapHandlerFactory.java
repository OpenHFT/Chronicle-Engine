package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.engine.server.internal.EngineWireHandler;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.network.*;
import net.openhft.chronicle.network.api.TcpHandler;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static net.openhft.chronicle.network.NetworkStatsListener.notifyHostPort;

public final class BootstrapHandlerFactory {
    private final Function<NetworkContext, ServerThreadingStrategy> threadingStrategyMapper;
    private final boolean notifyHostPort;
    private final NetworkStatsListener networkStatsListener;

    private BootstrapHandlerFactory(final Function<NetworkContext, ServerThreadingStrategy> threadingStrategyMapper, final boolean notifyHostPort, final NetworkStatsListener networkStatsListener) {
        this.threadingStrategyMapper = threadingStrategyMapper;
        this.notifyHostPort = notifyHostPort;
        this.networkStatsListener = networkStatsListener;
    }

    public static BootstrapHandlerFactory forEngineClusterContext(final NetworkStatsListener networkStatsListener) {
        return new BootstrapHandlerFactory(nc -> ServerThreadingStrategy.CONCURRENT, true, networkStatsListener);
    }

    static BootstrapHandlerFactory forServerEndpoint() {
        return new BootstrapHandlerFactory(NetworkContext::serverThreadingStrategy, false, null);
    }

    @NotNull
    public TcpEventHandler bootstrapHandlerFactory(final NetworkContext networkContext) {
        @NotNull final EngineWireNetworkContext nc = (EngineWireNetworkContext) networkContext;
        if (nc.isAcceptor())
            nc.wireOutPublisher(new VanillaWireOutPublisher(WireType.TEXT));
        networkContext.serverThreadingStrategy(threadingStrategyMapper.apply(nc));
        @NotNull final TcpEventHandler handler = new TcpEventHandler(networkContext);

        @NotNull final Function<Object, TcpHandler> consumer = o -> {

            if (o instanceof SessionDetailsProvider) {
                @NotNull final SessionDetailsProvider sessionDetails = (SessionDetailsProvider) o;
                nc.sessionDetails(sessionDetails);
                nc.wireType(sessionDetails.wireType());
                @Nullable final WireType wireType = nc.sessionDetails().wireType();
                if (wireType != null)
                    nc.wireOutPublisher().wireType(wireType);
                return new EngineWireHandler();
            } else if (o instanceof TcpHandler) {
                return (TcpHandler) o;
            }

            throw new UnsupportedOperationException("not supported class=" + o.getClass());
        };

        if (nc.networkStatsListener() == null)
            nc.networkStatsListener(networkStatsListener);

        if (notifyHostPort) {
            final NetworkStatsListener nl = nc.networkStatsListener();
            if (nl != null)
                notifyHostPort(nc.socketChannel(), nl);
        }

        @Nullable final Function<EngineWireNetworkContext, TcpHandler> f
                = x -> new HeaderTcpHandler<>(handler, consumer, x);

        @NotNull final WireTypeSniffingTcpHandler sniffer = new
                WireTypeSniffingTcpHandler<>(handler, f);

        handler.tcpHandler(sniffer);
        return handler;

    }
}
