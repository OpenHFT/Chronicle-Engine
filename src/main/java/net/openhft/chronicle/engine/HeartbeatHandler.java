/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.core.threads.Timer;
import net.openhft.chronicle.core.threads.VanillaEventHandler;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.network.ConnectionListener;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.cluster.ClusterContext;
import net.openhft.chronicle.network.cluster.HeartbeatEventHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * will periodically send a heatbeat message, the period of this message is defined by {@link
 * HeartbeatHandler#heartbeatIntervalMs} once the heart beat is
 *
 * @author Rob Austin.
 */
public class HeartbeatHandler<T extends EngineWireNetworkContext> extends AbstractSubHandler<T> implements
        Demarshallable, WriteMarshallable, HeartbeatEventHandler {

    static final Logger LOG = LoggerFactory.getLogger(SimpleEngineMain.class);

    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private final AtomicBoolean hasHeartbeats = new AtomicBoolean();
    private volatile long lastTimeMessageReceived;
    private ConnectionListener connectionMonitor;
    private Timer timer;
    private volatile long lastPing = 0;

    @UsedViaReflection
    protected HeartbeatHandler(@NotNull WireIn w) {
        heartbeatTimeoutMs = w.read(() -> "heartbeatTimeoutMs").int64();
        heartbeatIntervalMs = w.read(() -> "heartbeatIntervalMs").int64();
        assert heartbeatTimeoutMs >= 1000 :
                "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", this is too small";
        assert heartbeatIntervalMs >= 500 :
                "heartbeatIntervalMs=" + heartbeatIntervalMs + ", this is too small";
        onMessageReceived();

    }

    private HeartbeatHandler(long heartbeatTimeoutMs, long heartbeatIntervalMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        assert heartbeatTimeoutMs > heartbeatIntervalMs :
                "heartbeatIntervalMs=" + heartbeatIntervalMs + ", " +
                        "heartbeatTimeoutMs=" + heartbeatTimeoutMs;

        assert heartbeatTimeoutMs >= 1000 :
                "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", this is too small";
        assert heartbeatIntervalMs >= 500 :
                "heartbeatIntervalMs=" + heartbeatIntervalMs + ", this is too small";
    }

    private static WriteMarshallable heartbeatHandler(final long heartbeatTimeoutMs,
                                                      final long heartbeatIntervalMs,
                                                      final long cid) {
        return w -> w.writeDocument(true,
                d -> d.writeEventName(CoreFields.csp).text("/")
                        .writeEventName(CoreFields.cid).int64(cid)
                        .writeEventName(CoreFields.handler).typedMarshallable(new
                                HeartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs)));
    }

    @Override
    public void onInitialize(WireOut outWire) {

        if (nc().isAcceptor())
            heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs, cid()).writeMarshallable
                    (outWire);

        final WriteMarshallable heartbeatMessage = w -> {
            w.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
            w.writeDocument(false, d -> d.write(() -> "heartbeat").text(""));
        };

        connectionMonitor = nc().acquireConnectionListener();
        timer = new Timer(nc().rootAsset().findOrCreateView(EventLoop.class));
        startPeriodicHeartbeatCheck();
        startPeriodicallySendingHeartbeats(heartbeatMessage);
    }

    private void startPeriodicallySendingHeartbeats(WriteMarshallable heartbeatMessage) {

        final VanillaEventHandler task = () -> {
            if (isClosed())
                throw new InvalidEventHandlerException("closed");
            // we will only publish a heartbeat if the wire out publisher is empty
            WireOutPublisher wireOutPublisher = nc().wireOutPublisher();
            if (wireOutPublisher.isEmpty())
                wireOutPublisher.publish(heartbeatMessage);
            return true;
        };

        timer.scheduleAtFixedRate(task, this.heartbeatIntervalMs, this.heartbeatIntervalMs);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut w) {
        w.write(() -> "heartbeatTimeoutMs").int64(heartbeatTimeoutMs);
        assert heartbeatIntervalMs > 0;
        w.write(() -> "heartbeatIntervalMs").int64(heartbeatIntervalMs);
    }

    @Override
    public void onRead(@NotNull WireIn inWire, @NotNull WireOut outWire) {
        if (!inWire.hasMore())
            return;
        inWire.read(() -> "heartbeat").text();
    }

    @Override
    public void close() {
        if (connectionMonitor != null)
            connectionMonitor.onDisconnected(localIdentifier(), remoteIdentifier());
        if (closable().isClosed())
            return;
        lastTimeMessageReceived = Long.MAX_VALUE;
        Closeable.closeQuietly(closable());
    }

    public void onMessageReceived() {
        lastTimeMessageReceived = System.currentTimeMillis();

        long currentSecond = TimeUnit.MILLISECONDS.toSeconds(lastTimeMessageReceived);
        if (lastPing != currentSecond) {
            LOG.info(Integer.toHexString(hashCode()) + " lastTimeMessageReceived=" + lastTimeMessageReceived);
            lastPing = currentSecond;
        }
    }

    private VanillaEventHandler heartbeatCheck() {

        return () -> {

            if (HeartbeatHandler.this.closable().isClosed())
                throw new InvalidEventHandlerException("closed");

            boolean hasHeartbeats = hasReceivedHeartbeat();
            boolean prev = this.hasHeartbeats.getAndSet(hasHeartbeats);

            if (hasHeartbeats != prev) {
                if (!hasHeartbeats) {
                    connectionMonitor.onDisconnected(HeartbeatHandler.this.localIdentifier(), HeartbeatHandler.this.remoteIdentifier());
                    System.out.println("Heartbeat closing connection" + nc().sessionDetails());
                    HeartbeatHandler.this.close();

                    final Runnable socketReconnector = nc().socketReconnector();

                    // if we have been terminated then we should not attempt to reconnect
                    if (nc().terminationEventHandler().isTerminated() && socketReconnector != null)
                        socketReconnector.run();

                    throw new InvalidEventHandlerException("closed");
                } else
                    connectionMonitor.onConnected(HeartbeatHandler.this.localIdentifier(), HeartbeatHandler.this.remoteIdentifier());
            }

            return true;
        };
    }

    /**
     * periodically check that messages have been received, ie heartbeats
     */
    private void startPeriodicHeartbeatCheck() {
        timer.scheduleAtFixedRate(heartbeatCheck(), 0, heartbeatTimeoutMs);
    }

    /**
     * called periodically to check that the heartbeat has been received
     *
     * @return {@code true} if we have received a heartbeat recently
     */
    private boolean hasReceivedHeartbeat() {
        long currentTimeMillis = System.currentTimeMillis();
        boolean result = lastTimeMessageReceived + heartbeatTimeoutMs >= currentTimeMillis;

        if (!result)
            LOG.error(Integer.toHexString(hashCode()) + " missed heartbeat, lastTimeMessageReceived=" + lastTimeMessageReceived
                    + ", currentTimeMillis=" + currentTimeMillis);
        return result;
    }

    public static class Factory implements Function<ClusterContext, WriteMarshallable>,
            Demarshallable {

        @UsedViaReflection
        private Factory(WireIn w) {
        }

        @Override
        public WriteMarshallable apply(ClusterContext clusterContext) {
            long heartbeatTimeoutMs = clusterContext.heartbeatTimeoutMs();
            long heartbeatIntervalMs = clusterContext.heartbeatIntervalMs();
            return heartbeatHandler(heartbeatTimeoutMs, heartbeatIntervalMs,
                    HeartbeatHandler.class.hashCode());
        }
    }
}
