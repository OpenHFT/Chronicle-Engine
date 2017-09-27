package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.EngineWireNetworkContext;
import net.openhft.chronicle.map.Replica;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.network.cluster.AbstractSubHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.chronicle.enterprise.map.cluster.BiDirectionalMapReplicationHandler;
import software.chronicle.enterprise.map.cluster.EventKeys;
import software.chronicle.enterprise.map.cluster.ReplicationEventHandler;
import software.chronicle.enterprise.map.cluster.acknowledgement.ReplicationState;
import software.chronicle.enterprise.map.cluster.events.NoOpReplicationEventListener;
import software.chronicle.enterprise.map.cluster.events.ReplicationEventListener;
import software.chronicle.enterprise.map.log.NoOpReplicationEventsSentLog;
import software.chronicle.enterprise.map.log.ReplicationEventsSentLog;

import java.nio.ByteBuffer;
import java.util.concurrent.RejectedExecutionException;

public final class MapReplicationHandler extends AbstractSubHandler<EngineWireNetworkContext> implements Marshallable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BiDirectionalMapReplicationHandler.class);

    private long lastReadTimestamp;
    private ReplicatedChronicleMap<?, ?, ?> localMap;
    private transient Bytes<ByteBuffer> copyBuffer = Bytes.elasticByteBuffer();
    private ReplicationEventListener eventListener;
    private ReplicationEventsSentLog sentEventsLog;
    // TODO could be serialised/initialised in constructor
    private ReplicationState replicationState;

    private MapReplicationHandler(final long lastReadTimestamp) {
        this.lastReadTimestamp = lastReadTimestamp;
    }

    @Override
    public void onRead(@NotNull final WireIn inWire, @NotNull final WireOut outWire) {
        final StringBuilder eventName = Wires.acquireStringBuilder();
        @NotNull final ValueIn valueIn = inWire.readEventName(eventName);
        assignPostInitState();

        System.out.printf("%s [%d->%d]/ Read event %s%n", this, remoteIdentifier(), localIdentifier(), eventName);

        if (EventKeys.bootstrapTimestamp.contentEquals(eventName)) {
            final long lastReadTimestamp = valueIn.int64();

            localMap.setRemoteNodeCouldBootstrapFrom((byte) remoteIdentifier(), lastReadTimestamp);
            if (sentEventsLog != null) {
                sentEventsLog.onBootstrapTimeReceived(lastReadTimestamp);
            }
        } else if (EventKeys.replicationEvent.contentEquals(eventName)) {
            copyBuffer.clear();
            valueIn.bytes(copyBuffer);
            final long readPosition = copyBuffer.readPosition();
            eventListener.eventsReceived(localIdentifier(), remoteIdentifier(), copyBuffer.readRemaining(), 1);

            localMap.readExternalEntry(copyBuffer, remoteIdentifierAsByte());
            copyBuffer.readPosition(readPosition);
            if (sentEventsLog != null) {
                sentEventsLog.onReplicatedEntryReceived(copyBuffer);
            }
        } else if (EventKeys.replicationSequence.contentEquals(eventName)) {
            final long sequenceNumber = valueIn.int64();

            acknowledgeReplicationSequence(sequenceNumber);
        } else if (EventKeys.replicationSequenceAck.contentEquals(eventName)) {
            final long sequenceNumber = valueIn.int64();

            replicationState.replicationSequenceReceivedFromRemote(sequenceNumber);
        }
    }

    @Override
    public void onInitialize(final WireOut outWire) throws RejectedExecutionException {

        localMap = null;//nc().getReplicatedMapSupplier().get().getReplicatedMap();

        @NotNull Asset rootAsset = nc().rootAsset();
        @NotNull final RequestContext requestContext = RequestContext.requestContext(csp());
        @NotNull final Asset asset = rootAsset.acquireAsset(requestContext.fullName());

        final MapView<?, ?> mapView =
                asset.acquireMap(csp(), requestContext.keyType(), requestContext.valueType(), requestContext.cluster());
        final KeyValueStore kvStore = ((VanillaMapView) mapView).kvStore();
        localMap = (ReplicatedChronicleMap<?, ?, ?>) ((ChronicleMapKeyValueStore) kvStore).chronicleMap();

        System.out.printf("%s [%d->%d]/ Retrieved a map from the asset tree: 0x%s%n",
                this, localIdentifier(), remoteIdentifier(),
                Integer.toHexString(System.identityHashCode(localMap)));

        this.eventListener = NoOpReplicationEventListener.INSTANCE;//nc().clusterContext().getReplicationEventListener();

        if (nc().isAcceptor()) {
            final long lastReadTimestamp = this.localMap.
                    remoteNodeCouldBootstrapFrom(remoteIdentifierAsByte());
            @NotNull WriteMarshallable writeMarshallable = newReplicationHandler(lastReadTimestamp, csp(), cid());

            System.out.printf("%s [%d->%d]/publishing handler back to remote end%n",
                    this, localIdentifier(), remoteIdentifier());

            publish(writeMarshallable);
        }
        final ReplicatedChronicleMap<?, ?, ?>.ModificationIterator modificationIterator =
                this.localMap.acquireModificationIterator(remoteIdentifierAsByte());
        final EventLoop eventLoop = nc().eventLoop();
        modificationIterator.setModificationNotifier(eventLoop::unpause);
        modificationIterator.dirtyEntries(lastReadTimestamp);

        assignPostInitState();

        final Replica.ModificationIterator wrapped = new LoggingModificationIterator(modificationIterator, localIdentifier(), remoteIdentifier());

        System.out.printf("Adding an event handler [%d->%d]%n",
                localIdentifier(), remoteIdentifier());
        eventLoop.addHandler(true,
                new ReplicationEventHandler(wrapped, this.localMap, this::nc, cid(), this::close, sentEventsLog, replicationState));
//        }
    }

    private static final class LoggingModificationIterator implements Replica.ModificationIterator {
        private final Replica.ModificationIterator delegate;
        private final int local;
        private final int remote;
        private boolean lastResult;
        private long pollCount;

        private LoggingModificationIterator(final Replica.ModificationIterator delegate, final int local, final int remote) {
            this.delegate = delegate;
            this.local = local;
            this.remote = remote;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public boolean nextEntry(@NotNull final Callback callback, final int i) {
            final boolean entry = delegate.nextEntry(callback, i);
            pollCount++;
            if (entry != lastResult) {
                System.out.printf("[%d->%d] nextEntry: %s (polls: %d)%n",
                        local, remote, entry, pollCount);
                pollCount = 0;
                lastResult = entry;
            }

            return entry;
        }

        @Override
        public void dirtyEntries(final long l) {
            System.out.printf("[%d->%d] dirtyEntries: %d%n",
                    local, remote, l);
            delegate.dirtyEntries(l);
        }

        @Override
        public void setModificationNotifier(@NotNull final Replica.ModificationNotifier modificationNotifier) {
            delegate.setModificationNotifier(modificationNotifier);
        }
    }

    @NotNull
    public static WriteMarshallable newReplicationHandler(final long lastReadTimestamp, final String csp, final long cid) {
        @NotNull final MapReplicationHandler handler = new MapReplicationHandler(lastReadTimestamp);

        return w -> w.writeDocument(true, d -> d.writeEventName(CoreFields.csp).text(csp)
                .writeEventName(CoreFields.cid).int64(cid)
                .writeEventName(CoreFields.handler).typedMarshallable(handler));
    }

    @Override
    public void readMarshallable(@NotNull final WireIn wire) throws IORuntimeException {
        lastReadTimestamp = wire.read(EventKeys.initialBootstrapTimestamp).int64();
        copyBuffer = Bytes.elasticByteBuffer();
    }

    @Override
    public void writeMarshallable(@NotNull final WireOut wire) {
        wire.write(EventKeys.initialBootstrapTimestamp).int64(lastReadTimestamp);
    }

    @Override
    public void close() {
        if (copyBuffer.refCount() != 0) {
            copyBuffer.release();
        }
        super.close();
    }

    private void acknowledgeReplicationSequence(final long sequenceNumber) {
        nc().wireOutPublisher().put(null, wireOut -> {
            wireOut.writeDocument(true, d -> d.write(CoreFields.cid).int64(cid()));
            wireOut.writeDocument(false,
                    d -> d.writeEventName(EventKeys.replicationSequenceAck).int64(sequenceNumber));
        });
    }

    private byte remoteIdentifierAsByte() {
        return (byte) super.remoteIdentifier();
    }

    private void assignPostInitState() {
        sentEventsLog = NoOpReplicationEventsSentLog.INSTANCE;
        replicationState = new ReplicationState();
//        if (replicationState == null) {
//            replicationState = nc().getReplicationState(nc().localIdentifier(), remoteIdentifierAsByte());
//        }
//        if (sentEventsLog == null && nc() != null) {
//            final ReplicatedMapCfg<?, ?> mapConfig = nc().mapConfig();
//            if (mapConfig.enableReplicationLogging()) {
//                final String logBaseDirectory = mapConfig.mapLogDirectory().orElseGet(() -> {
//                    LOGGER.warn("Map configuration mapLogDirectory not configured, using current directory");
//                    return Paths.get(".").toFile().getAbsolutePath();
//                });
//
//                final Path logDir = Paths.get(logBaseDirectory, nc().isAcceptor() ? "outbound" : "inbound", Byte.toString(nc().getLocalHostIdentifier()));
//                sentEventsLog = QueueReplicationEventsSentLog.binaryQueue(logDir.toString(), remoteIdentifierAsByte());
//            } else {
//                sentEventsLog = NoOpReplicationEventsSentLog.INSTANCE;
//            }
//        }
    }
}
