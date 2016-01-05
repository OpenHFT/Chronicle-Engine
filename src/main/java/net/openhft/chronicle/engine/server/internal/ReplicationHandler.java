package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.Jvm;
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

            // receives replication events
            if (CoreFields.lastUpdateTime.contentEquals(eventName)) {
                if (Jvm.isDebug())
                    System.out.println("server : received lastUpdateTime");
                final long time = valueIn.int64();
                final byte id = inWire.read(() -> "id").int8();
                replication.setLastModificationTime(id, time);
                return;
            }

            // receives replication events
            if (replicationEvent.contentEquals(eventName)) {
                if (Jvm.isDebug() && LOG.isDebugEnabled())
                    LOG.debug("server : received replicationEvent");
                ReplicationEntry replicatedEntry = valueIn.typedMarshallable();
                assert replicatedEntry != null;

                if (Jvm.isDebug() && LOG.isDebugEnabled())
                    LOG.debug("*****\t\t\t\t ->  RECEIVED : SERVER : replication latency=" + (System
                            .currentTimeMillis() - replicatedEntry.timestamp()) + "ms  ");

                replication.applyReplication(replicatedEntry);
                return;
            }

            assert outWire != null;
            outWire.writeDocument(true, wire -> outWire.writeEventName(CoreFields.tid).int64(tid));

            if (identifier.contentEquals(eventName))
                writeData(inWire.bytes(), out -> outWire.write(identifierReply).int8(hostId.hostId()));

            if (bootstrap.contentEquals(eventName)) {
                writeData(true, inWire.bytes(), out -> {
                    if (LOG.isDebugEnabled())
                        LOG.debug("server : received bootstrap request");

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
                    outWire.writeEventName(bootstrap).typedMarshallable(outBootstrap);

                    if (Jvm.isDebug())
                        System.out.println("server : received replicationSubscribe");

                    // receive bootstrap
                    if (mi == null)
                        return;
                    // sends replication events back to the remote client
                    mi.setModificationNotifier(eventLoop::unpause);

                    eventLoop.addHandler(true, new EventHandler() {

                        boolean hasSentLastUpdateTime;
                        long lastUpdateTime = 0;
                        boolean hasLogged = false;
                        int count = 0;
                        long startBufferFullTimeStamp = 0;

                        @Override
                        public boolean action() throws InvalidEventHandlerException {
                            if (connectionClosed)
                                throw new InvalidEventHandlerException();

                            final WireOutPublisher publisher = ReplicationHandler.this.publisher;
                            //noinspection SynchronizationOnLocalVariableOrMethodParameter
                            synchronized (publisher) {
                                // given the sending an event to the publish hold the chronicle map lock
                                // we will send only one at a time


                                if (!publisher.canTakeMoreData()) {
                                    if (mi.hasNext() && startBufferFullTimeStamp == 0) {
                                        startBufferFullTimeStamp = System.currentTimeMillis();
                                        return false;
                                    }
                                }


                                if (!mi.hasNext()) {

                                    if (startBufferFullTimeStamp != 0) {
                                        long timetaken = System.currentTimeMillis() - startBufferFullTimeStamp;
                                        if (timetaken > 100)
                                            LOG.info("blocked - outbound buffer full=" + timetaken + "ms");
                                        startBufferFullTimeStamp = 0;
                                    }

                                    // because events arrive in a bitset ( aka random ) order ( not necessary in
                                    // time order ) we can only be assured that the latest time of
                                    // the last event is really the latest time, once all the events
                                    // have been received, we know when we have received all events
                                    // when there are no more events to process.
                                    if (!hasSentLastUpdateTime && lastUpdateTime > 0) {
                                        publisher.put(null, publish -> publish
                                                .writeNotReadyDocument(false,
                                                        wire -> {
                                                            wire.writeEventName(CoreFields.lastUpdateTime).int64(lastUpdateTime);
                                                            wire.write(() -> "id").int8(id);
                                                        }
                                                ));

                                        hasSentLastUpdateTime = true;

                                        if (!hasLogged) {
                                            LOG.info("received ALL replication the EVENTS for " +
                                                    "id=" + id);
                                            hasLogged = true;
                                        }

                                        return false;
                                    }
                                }

                                mi.nextEntry(e -> publisher.put(null, publish1 -> {

                                    if (e.remoteIdentifier() == hostId.hostId())
                                        return;

                                    long newlastUpdateTime = Math.max(lastUpdateTime, e.timestamp());

                                    if (newlastUpdateTime > lastUpdateTime) {
                                        hasSentLastUpdateTime = false;
                                        lastUpdateTime = newlastUpdateTime;
                                    }

                                    if (LOG.isDebugEnabled())
                                        LOG.debug("publish from server response from iterator " +
                                                "localIdentifier=" + hostId + " ,remoteIdentifier=" +
                                                id + " event=" + e);

                                    publish1.writeNotReadyDocument(true,
                                            wire -> wire.writeEventName(CoreFields.tid).int64(inputTid));

                                    if (LOG.isInfoEnabled()) {
                                        long delay = System.currentTimeMillis() - e.timestamp();
                                        if (delay > 60) {
                                            LOG.info("Snt Srv latency=" + delay + "ms\t");
                                            if (count++ % 10 == 1)
                                                LOG.info("");
                                        }
                                    }
                                    if (publish1.bytes().writePosition() > 100000 && LOG.isDebugEnabled())
                                        LOG.debug(publish1.bytes().toDebugString(128));
                                    publish1.writeNotReadyDocument(false,
                                            wire -> wire.writeEventName(replicationEvent).typedMarshallable(e));

                                }));
                            }
                            return true;
                        }
                    });
                });
            }
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
