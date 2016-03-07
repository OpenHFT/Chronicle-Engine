package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import org.jetbrains.annotations.NotNull;

/**
 * Created by rob austin on 28/06/2015.
 */
public class RemoteTopologySubscription extends AbstractRemoteSubscription<TopologicalEvent> implements TopologySubscription {

    public RemoteTopologySubscription(@NotNull RequestContext requestContext, @NotNull Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(requestContext));
    }

    @NotNull
    private static String toUri(@NotNull final RequestContext context) {
        return context.fullName() + "?view=topologySubscription";
    }

    @Override
    public void notifyEvent(TopologicalEvent event) {
        throw new UnsupportedOperationException("Remote client should not attempt to notify of a change");
    }

}
