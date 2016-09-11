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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.api.session.SubHandler;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.cluster.WritableSubHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
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

    final List<WriteMarshallable> writers = new ArrayList<>();
    @NotNull
    private final Map<Long, SubHandler> cidToHandle = new HashMap<>();
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
}
