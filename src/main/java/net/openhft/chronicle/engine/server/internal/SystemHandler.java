package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.cfg.UserStat;
import net.openhft.chronicle.network.ClientClosedProvider;
import net.openhft.chronicle.network.SessionMode;
import net.openhft.chronicle.network.api.session.SessionDetailsProvider;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.SystemHandler.EventId.heartbeat;
import static net.openhft.chronicle.engine.server.internal.SystemHandler.EventId.onClientClosing;

/**
 * @author Rob Austin.
 */
public class SystemHandler extends AbstractHandler implements ClientClosedProvider {
    private final StringBuilder eventName = new StringBuilder();
    private SessionDetailsProvider sessionDetails;
    private final WireParser wireParser = wireParser();
    @Nullable
    private Map<String, UserStat> monitoringMap;
    private volatile boolean hasClientClosed;
    private boolean wasHeartBeat;
    @NotNull
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, tid) -> {
        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (EventId.userId.contentEquals(eventName)) {
            this.sessionDetails.setUserId(valueIn.text());
            if (this.monitoringMap != null) {
                UserStat userStat = new UserStat();
                userStat.setLoggedIn(LocalTime.now());
                monitoringMap.put(sessionDetails.userId(), userStat);
            }

            while (inWire.bytes().readRemaining() > 0)
                wireParser.parseOne(inWire, null);

            return;
        }

        if (!heartbeat.contentEquals(eventName) && !onClientClosing.contentEquals(eventName)) {
            return;
        }

        wasHeartBeat = true;

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

    public boolean wasHeartBeat() {
        return wasHeartBeat;
    }

    void process(@NotNull final WireIn inWire,
                 @NotNull final WireOut outWire, final long tid,
                 @NotNull final SessionDetailsProvider sessionDetails,
                 @Nullable Map<String, UserStat> monitoringMap) {
        wasHeartBeat = false;
        this.sessionDetails = sessionDetails;
        this.monitoringMap = monitoringMap;
        setOutWire(outWire);
        dataConsumer.accept(inWire, tid);
    }

    private WireParser wireParser() {
        final WireParser parser = new VanillaWireParser((s, v, $) -> {
        });
        parser.register(() -> EventId.domain.toString(), (s, v, $) -> v.text(this, (o, x) -> o.sessionDetails.setDomain(x)));
        parser.register(() -> EventId.sessionMode.toString(), (s, v, $) -> v.text(this, (o, x) -> o
                .sessionDetails.setSessionMode(SessionMode.valueOf(x))));
        parser.register(() -> EventId.securityToken.toString(), (s, v, $) -> v.text(this, (o, x) -> o
                .sessionDetails.setSecurityToken(x)));
        parser.register(() -> EventId.clientId.toString(), (s, v, $) -> v.text(this, (o, x) -> o
                .sessionDetails.setClientId(UUID.fromString(x))));
        return parser;
    }

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
        userId,
        sessionMode,
        domain,
        securityToken,
        clientId
    }
}

