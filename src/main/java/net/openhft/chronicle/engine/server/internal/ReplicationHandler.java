package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.ReplicationHandler.EventId.*;

/**
 * Created by Rob Austin
 */
public class ReplicationHandler<E> extends AbstractHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationHandler.class);
    private final StringBuilder eventName = new StringBuilder();
    private Replication replication;
    private WireOutPublisher publisher;
    private HostIdentifier hostId;
    private long tid;

    private EventLoop eventLoop;

    @NotNull
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            if (replicationSubscribe.contentEquals(eventName)) {

                // receive bootstrap
                final byte id = valueIn.int8();
                final ModificationIterator mi = replication.acquireModificationIterator(id);
                if (mi == null)
                    return;
                // sends replication events back to the remote client
                mi.setModificationNotifier(eventLoop::unpause);

                eventLoop.addHandler(new EventHandler() {
                    @Override
                    public boolean action() throws InvalidEventHandlerException {
                        if (connectionClosed)
                            throw new InvalidEventHandlerException();

                        final AtomicBoolean hadNext = new AtomicBoolean();

                        mi.forEach(e -> publisher.put(null, publish1 -> {

                            if (e.remoteIdentifier() == hostId.hostId())
                                return;

                            hadNext.set(true);
                            if (LOG.isDebugEnabled())
                                LOG.debug("publish from server response from iterator " +
                                        "localIdentifier=" + hostId + " ,remoteIdentifier=" +
                                        id + " event=" + e);

                            publish1.writeDocument(true,
                                    wire -> wire.writeEventName(CoreFields.tid).int64(inputTid));

                            publish1.writeNotReadyDocument(false,
                                    wire -> wire.write(replicationEvent).typedMarshallable(e));

                        }));

                        return hadNext.get();
                    }
                });


                return;
            }

            // receives replication events
            if (replicationEvent.contentEquals(eventName)) {
                ReplicationEntry replicatedEntry = valueIn.typedMarshallable();
                assert replicatedEntry != null;
                replication.applyReplication(replicatedEntry);
                return;
            }

            assert outWire != null;
            outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

            writeData(inWire.bytes(), out -> {

                if (identifier.contentEquals(eventName)) {
                    outWire.write(identifierReply).int8(hostId.hostId());
                    return;
                }

                if (bootstrap.contentEquals(eventName)) {

                    // receive bootstrap
                    final Bootstrap inBootstrap = valueIn.typedMarshallable();
                    if (inBootstrap == null)
                        return;
                    final byte id = inBootstrap.identifier();

                    final ModificationIterator mi = replication.acquireModificationIterator(id);
                    if (mi != null)
                        mi.dirtyEntries(inBootstrap.lastUpdatedTime());

                    // send bootstrap
                    final Bootstrap outBootstrap = new Bootstrap();
                    outBootstrap.identifier(hostId.hostId());
                    outBootstrap.lastUpdatedTime(replication.lastModificationTime(id));
                    outWire.write(bootstrap).typedMarshallable(outBootstrap);
                }
            });
        }

    };

    void process(@NotNull final WireIn inWire,
                 final WireOutPublisher publisher,
                 final long tid,
                 final Wire outWire,
                 final HostIdentifier hostId,
                 final Replication replication,
                 final EventLoop eventLoop) {

        this.eventLoop = eventLoop;
        setOutWire(outWire);

        this.hostId = hostId;
        this.publisher = publisher;
        this.replication = replication;
        this.tid = tid;

        dataConsumer.accept(inWire, tid);

    }

    public enum EventId implements ParameterizeWireKey {
        publish,
        onEndOfSubscription,
        apply,
        replicationEvent,
        replicationSubscribe,
        bootstrap,
        identifierReply,
        identifier;

        private final WireKey[] params;

        @SafeVarargs
        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            //noinspection unchecked
            return (P[]) this.params;
        }
    }

}
