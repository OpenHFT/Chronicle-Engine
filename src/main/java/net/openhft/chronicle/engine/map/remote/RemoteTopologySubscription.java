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
