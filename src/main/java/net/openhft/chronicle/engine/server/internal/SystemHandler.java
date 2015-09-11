package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.cfg.UserStat;
import net.openhft.chronicle.network.ClientClosedProvider;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalTime;
import java.util.Map;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.SystemHandler.EventId.heartbeat;
import static net.openhft.chronicle.engine.server.internal.SystemHandler.EventId.onClientClosing;

/**
 * @author Rob Austin.
 */
public class SystemHandler extends AbstractHandler implements ClientClosedProvider {
    private final StringBuilder eventName = new StringBuilder();
    private SessionDetailsProvider sessionDetails;
    @Nullable
    private Map<String, UserStat> monitoringMap;
    private volatile boolean hasClientClosed;

    void process(@NotNull final WireIn inWire,
                 @NotNull final WireOut outWire, final long tid,
                 @NotNull final SessionDetailsProvider sessionDetails,
                 @Nullable Map<String, UserStat> monitoringMap) {
        this.sessionDetails = sessionDetails;
        this.monitoringMap = monitoringMap;
        setOutWire(outWire);
        dataConsumer.accept(inWire, tid);
    }

    @NotNull
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, tid) -> {

        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (EventId.userid.contentEquals(eventName)) {
            this.sessionDetails.setUserId(valueIn.text());
            if(this.monitoringMap != null){
                UserStat userStat = new UserStat();
                userStat.setLoggedIn(LocalTime.now());
                monitoringMap.put(sessionDetails.userId(), userStat);
            }
            return;
        }

        if (!heartbeat.contentEquals(eventName) && !onClientClosing.contentEquals(eventName))
            return;

        //noinspection ConstantConditions
        outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

        writeData(inWire.bytes(), out -> {

            if (heartbeat.contentEquals(eventName))
                outWire.write(EventId.heartbeatReply).int64(valueIn.int64());

            else if (onClientClosing.contentEquals(eventName)) {
                hasClientClosed = true;
                outWire.write(EventId.onClosingReply).text("");
            }
        });
    };

    /**
     * @return {@code true} if the client has intentionally closed
     */
    @Override
    public boolean hasClientClosed() {
        return hasClientClosed;
    }

    public enum EventId implements WireKey {
        heartbeat,
        heartbeatReply,
        onClientClosing,
        onClosingReply,
        userid
    }
}

