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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.cluster.WritableSubHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.openhft.chronicle.network.connection.CoreFields.csp;

/**
 * @author Rob Austin.
 */
abstract class CspTcpHander<T extends NetworkContext> extends WireTcpHandler<T> {

    protected final StringBuilder cspText = new StringBuilder();

    @NotNull
    private final Map<Long, SubHandler> cidToHandle = new HashMap<>();
    final List<WriteMarshallable> writers = new ArrayList<>();
    private SubHandler handler;
    private HeartbeatEventHandler heartbeatEventHandler;
    private long lastCid;

    SubHandler handler() {
        return handler;
    }


    @Override
    public void close() {
        cidToHandle.values().forEach(Closeable::closeQuietly);
        super.close();
    }

    /**
     * peeks the csp or if it has a cid converts the cid into a Csp and returns that
     *
     * @return {@code true} if if a csp was read rather than a cid
     */
    boolean readMeta(@NotNull final WireIn wireIn) {
        final StringBuilder event = Wires.acquireStringBuilder();

        ValueIn valueIn = wireIn.readEventName(event);


        if (csp.contentEquals(event)) {
            final String csp = valueIn.text();

            long cid;
            event.setLength(0);
            valueIn = wireIn.readEventName(event);

            if (CoreFields.cid.contentEquals(event))
                cid = valueIn.int64();
            else
                throw new IllegalStateException("expecting 'cid' but eventName=" + event);

            event.setLength(0);
            valueIn = wireIn.readEventName(event);

            if (CoreFields.handler.contentEquals(event)) {
                if (cidToHandle.containsKey(cid))
                    // already has it registered
                    return false;
                handler = valueIn.typedMarshallable();
                handler.nc(nc());
                handler.closeable(this);
                if (handler() instanceof HeartbeatEventHandler) {
                    assert heartbeatEventHandler == null : "its assumed that you will only have a " +
                            "single heartbeatReceiver per connection";
                    heartbeatEventHandler = (HeartbeatEventHandler) handler();
                }

                handler.cid(cid);
                handler.csp(csp);
                lastCid = cid;
                cidToHandle.put(cid, handler);

                if (handler instanceof WritableSubHandler)
                    writers.add(((WritableSubHandler) handler).writer());
            } else
                throw new IllegalStateException("expecting 'cid' but eventName=" + event);
            return true;
        } else if (CoreFields.cid.contentEquals(event)) {
            final long cid = valueIn.int64();
            if (cid == lastCid)
                return false;
            lastCid = cid;
            handler = cidToHandle.get(cid);

            if (handler == null) {
                throw new IllegalStateException("handler not found : for CID=" + cid + ", " +
                        "known cids=" + cidToHandle.keySet());
            }
        } else {
            throw new IllegalStateException("expecting either csp or cid, event=" + event);
        }

        return false;
    }

    HeartbeatEventHandler heartbeatEventHandler() {
        return heartbeatEventHandler;
    }

    @Override
    protected abstract void onRead(@NotNull WireIn in, @NotNull WireOut out);

}
