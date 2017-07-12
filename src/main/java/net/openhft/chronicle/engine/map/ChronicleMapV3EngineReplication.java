package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.hash.replication.ReplicableEntry;
import net.openhft.chronicle.map.Replica;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public final class ChronicleMapV3EngineReplication implements EngineReplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapV3EngineReplication.class);
    private final HostIdentifier hostIdentifier;
    private final long[] lastModificationTimeByRemoteIdentifier = new long[255];
    private ReplicatedChronicleMap<?, ?, ?> chronicleMap;

    public ChronicleMapV3EngineReplication(final RequestContext requestContext, @NotNull final Asset asset) {
        // TODO mark.price
        hostIdentifier = asset.findOrCreateView(HostIdentifier.class);

        assert hostIdentifier != null;
        LOGGER.info("Created replication for asset {} with host {}", asset.fullName(), hostIdentifier.hostId());
    }

    @Override
    public void applyReplication(@NotNull final ReplicationEntry replicatedEntry) {
        LOGGER.info("ReplicationEntry: {}", replicatedEntry);
    }

    @Override
    public byte identifier() {
        LOGGER.info("Returning identifier");
        return hostIdentifier.hostId();
    }

    @Nullable
    @Override
    public ModificationIterator acquireModificationIterator(final byte remoteIdentifier) {
        return new ModificationIteratorAdaptor(chronicleMap.acquireModificationIterator(remoteIdentifier),
                chronicleMap, remoteIdentifier);
    }

    @Override
    public long lastModificationTime(final byte remoteIdentifier) {
        LOGGER.info("No lastModificationTime for you");

        return lastModificationTimeByRemoteIdentifier[remoteIdentifier];
    }

    @Override
    public void setLastModificationTime(final byte identifier, final long timestamp) {
        LOGGER.info("setLastModificationTime {}, {}", identifier, timestamp);
        lastModificationTimeByRemoteIdentifier[identifier] = timestamp;
    }

    <K, V> void setChronicleMap(final ReplicatedChronicleMap<K, V, ?> chronicleMap) {
        this.chronicleMap = chronicleMap;
    }

    private static class ModificationIteratorAdaptor implements ModificationIterator {
        private static final Logger LOGGER = LoggerFactory.getLogger(ModificationIteratorAdaptor.class);
        private final ReplicatedChronicleMap.ModificationIterator modificationIterator;
        private final ReplicatedChronicleMap<?, ?, ?> chronicleMap;
        private final byte remoteIdentifier;

        ModificationIteratorAdaptor(final ReplicatedChronicleMap.ModificationIterator modificationIterator,
                                    final ReplicatedChronicleMap<?, ?, ?> chronicleMap, final byte remoteIdentifier) {
            this.modificationIterator = modificationIterator;
            this.chronicleMap = chronicleMap;
            this.remoteIdentifier = remoteIdentifier;
        }

        @Override
        public boolean hasNext() {
            final boolean moreToProcess = modificationIterator.hasNext();
            if (moreToProcess) {
                LOGGER.info("hasNext()!");
            }
            return moreToProcess;
        }

        @Override
        public boolean nextEntry(final Consumer<ReplicationEntry> consumer) {
            LOGGER.info("nextEntry()!");
            return modificationIterator.nextEntry(new Replica.ModificationIterator.Callback() {
                long bootstrapTime;
                @Override
                public void onEntry(final ReplicableEntry replicableEntry, final int chronicleId) {
                    consumer.accept(convert(replicableEntry, chronicleId, bootstrapTime, remoteIdentifier));
                }

                @Override
                public void onBootstrapTime(final long bootstrapTime, final int chronicleId) {
                    this.bootstrapTime = bootstrapTime;
                }
            }, 0);
        }

        private ReplicationEntry convert(final ReplicableEntry replicableEntry, final int chronicleId,
                                         final long bootstrapTime, final byte remoteIdentifier) {
            final Bytes<ByteBuffer> payload = Bytes.elasticByteBuffer();
            final Bytes<ByteBuffer> destination = Bytes.elasticByteBuffer();
            chronicleMap.writeExternalEntry(replicableEntry, null, destination, chronicleId);
            final boolean isDeletedNotYetSupported = false;
            return new MapV3ReplicationEntry(payload, destination,
                    replicableEntry.originTimestamp(), replicableEntry.originIdentifier(),
                    isDeletedNotYetSupported, bootstrapTime, remoteIdentifier);
        }

        @Override
        public void dirtyEntries(final long fromTimeStamp) {
            modificationIterator.dirtyEntries(fromTimeStamp);
        }

        @Override
        public void setModificationNotifier(@NotNull final ModificationNotifier modificationNotifier) {
            modificationIterator.setModificationNotifier(modificationNotifier::onChange);
        }
    }

    private static final class MapV3ReplicationEntry implements ReplicationEntry {
        private final Bytes<ByteBuffer> key;
        private final Bytes<ByteBuffer> value;
        private final long originTimestamp;
        private final byte originIdentifier;
        private final boolean isDeleted;
        private final long bootstrapTime;
        private final byte remoteIdentifier;

        public MapV3ReplicationEntry(final Bytes<ByteBuffer> key, final Bytes<ByteBuffer> value,
                                     final long originTimestamp, final byte originIdentifier,
                                     final boolean isDeleted, final long bootstrapTime,
                                     final byte remoteIdentifier) {
            this.key = key;
            this.value = value;
            this.originTimestamp = originTimestamp;
            this.originIdentifier = originIdentifier;
            this.isDeleted = isDeleted;
            this.bootstrapTime = bootstrapTime;
            this.remoteIdentifier = remoteIdentifier;
        }

        @Nullable
        @Override
        public BytesStore key() {
            return key;
        }

        @Nullable
        @Override
        public BytesStore value() {
            return value;
        }

        @Override
        public long timestamp() {
            return originTimestamp;
        }

        @Override
        public byte identifier() {
            return originIdentifier;
        }

        @Override
        public byte remoteIdentifier() {
            return remoteIdentifier;
        }

        @Override
        public boolean isDeleted() {
            return isDeleted;
        }

        @Override
        public long bootStrapTimeStamp() {
            return bootstrapTime;
        }
    }
}