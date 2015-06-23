package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.map.replication.Bootstrap;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.bootstap;
import static net.openhft.chronicle.engine.server.internal.PublisherHandler.Params.message;
import static net.openhft.chronicle.engine.server.internal.ReplicationHandler.EventId.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public class ReplicationHandler<E> extends AbstractHandler {
    private final StringBuilder eventName = new StringBuilder();
    private Replication replication;
    private Queue<Consumer<Wire>> publisher;

    private HostIdentifier hostId;
    private long tid;


    void process(final Wire inWire,
                 final Queue<Consumer<Wire>> publisher,
                 final long tid,
                 final Wire outWire,
                 HostIdentifier hostId,
                 Replication replication) {
        setOutWire(outWire);

        this.hostId = hostId;
        this.publisher = publisher;
        this.replication = replication;
        this.tid = tid;
        dataConsumer.accept(inWire, tid);

    }

    public enum Params implements WireKey {
        entry, message;
    }

    public enum EventId implements ParameterizeWireKey {
        publish,
        onEndOfSubscription,
        apply,
        replicationEvent,
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

    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            outWire.writeDocument(true, wire -> outWire.writeEventName(net.openhft.chronicle.network.connection.CoreFields.tid).int64(tid));

            writeData(out -> {

                if (identifier.contentEquals(eventName)) {
                    outWire.writeEventName(reply).int8(hostId.hostId());
                    return;
                }

                // receives replication events
                if (replicationEvent.contentEquals(eventName)) {
                    replication.applyReplication(inWire.read(Params.entry).typedMarshallable());
                    return;
                }

                if (bootstap.contentEquals(eventName)) {

                    // receive bootstrap
                    final Bootstrap inBootstrap = valueIn.typedMarshallable();
                    final byte id = inBootstrap.identifier();
                    final EngineReplication.ModificationIterator mi = replication.acquireModificationIterator(id);

                    // sends replication events back to the remote client
                    mi.setModificationNotifier(() -> {
                        try {
                            mi.forEach(e -> publisher.add(publish -> {

                                publish.writeDocument(true,
                                        wire -> wire.writeEventName(net.openhft.chronicle.network.connection.CoreFields.tid).int64(inputTid));

                                publish.writeDocument(false,
                                        wire -> wire.write(reply).typedMarshallable(null));

                            }));
                        } catch (InterruptedException e) {
                            Jvm.rethrow(e);
                        }

                    });

                    // send bootstrap
                    final Bootstrap outBootstrap = new Bootstrap();
                    outBootstrap.identifier(hostId.hostId());
                    outBootstrap.lastUpdatedTime(replication.lastModificationTime(id));
                    outWire.write(reply).typedMarshallable(outBootstrap);
                    return;
                }
            });
        }

    };

}
