package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.EngineReplication.ModificationIterator;
import net.openhft.chronicle.engine.api.EngineReplication.ReplicationEntry;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.bootstap;
import static net.openhft.chronicle.engine.server.internal.ReplicationHandler.EventId.*;

/**
 * Created by Rob Austin
 */
public class ReplicationHandler<E> extends AbstractHandler {
    private final StringBuilder eventName = new StringBuilder();
    private Replication replication;
    private Queue<Consumer<Wire>> publisher;

    private HostIdentifier hostId;
    private long tid;
    private AtomicBoolean isClosed;
    private EventLoop eventLoop;

    void process(@NotNull final WireIn inWire,
                 final Queue<Consumer<Wire>> publisher,
                 final long tid,
                 final Wire outWire,
                 final HostIdentifier hostId,
                 final Replication replication,
                 final AtomicBoolean isClosed,
                 final EventLoop eventLoop) {
        this.isClosed = isClosed;
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
        replicationReply,
        bootstrapReply,
        identifierReply,
        identifier;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }

    @Nullable
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            if (replicationSubscribe.contentEquals(eventName)) {

                // receive bootstrap
                final byte id = valueIn.int8();
                final ModificationIterator mi = replication.acquireModificationIterator(id);

                // sends replication events back to the remote client
                mi.setModificationNotifier(() -> {

                    mi.forEach(e -> publisher.add(publish -> {

                        publish.writeDocument(true,
                                wire -> wire.writeEventName(CoreFields.tid).int64(inputTid));

                        publish.writeNotReadyDocument(false,
                                wire -> wire.write(replicationReply).typedMarshallable(e));

                    }));
                });

                mi.setModificationNotifier(eventLoop::unpause);

                eventLoop.addHandler(new EventHandler() {
                    @Override
                    public boolean action() throws InvalidEventHandlerException {

                        if (isClosed.get())
                            throw new InvalidEventHandlerException();

                        if (!mi.hasNext())
                            return false;

                        mi.forEach(e -> publisher.add(publish1 -> {

                            publish1.writeDocument(true,
                                    wire -> wire.writeEventName(CoreFields.tid).int64(inputTid));

                            publish1.writeNotReadyDocument(false,
                                    wire -> wire.write(replicationReply).typedMarshallable(e));

                        }));
                        return true;

                    }

                    @NotNull
                    @Override
                    public HandlerPriority priority() {
                        return HandlerPriority.MEDIUM;
                    }
                });

                try {
                    mi.dirtyEntries(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return;
            }

            // receives replication events
            if (replicationEvent.contentEquals(eventName)) {
                ReplicationEntry replicatedEntry = valueIn.typedMarshallable();
                replication.applyReplication(replicatedEntry);
                return;
            }

            outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

            writeData(inWire.bytes(), out -> {

                if (identifier.contentEquals(eventName)) {
                    outWire.write(identifierReply).int8(hostId.hostId());
                    return;
                }

                if (bootstap.contentEquals(eventName)) {

                    // receive bootstrap
                    final Bootstrap inBootstrap = valueIn.typedMarshallable();
                    final byte id = inBootstrap.identifier();

                    // send bootstrap
                    final Bootstrap outBootstrap = new Bootstrap();
                    outBootstrap.identifier(hostId.hostId());
                    outBootstrap.lastUpdatedTime(replication.lastModificationTime(id));
                    outWire.write(bootstrapReply).typedMarshallable(outBootstrap);
                }
            });
        }

    };

}
