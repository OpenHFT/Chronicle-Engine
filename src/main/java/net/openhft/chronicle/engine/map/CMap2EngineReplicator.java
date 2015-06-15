package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.hash.replication.EngineReplicationLangBytesConsumer;
import net.openhft.chronicle.map.EngineReplicationLangBytes;
import net.openhft.chronicle.map.EngineReplicationLangBytes.EngineModificationIterator;
import net.openhft.chronicle.network.connection.TcpConnectionHub;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.Consumer;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.lang.io.NativeBytes.wrap;

/**
 * Created by Rob Austin
 */
public class CMap2EngineReplicator implements EngineReplication,
        EngineReplicationLangBytesConsumer, View {

    private final RequestContext context;
    private EngineReplicationLangBytes engineReplicationLang;
    private final ThreadLocal<PointerBytesStore> keyLocal = withInitial(PointerBytesStore::new);
    private final ThreadLocal<PointerBytesStore> valueLocal = withInitial(PointerBytesStore::new);

    public CMap2EngineReplicator(RequestContext requestContext, Asset asset) {
        this(requestContext);
    }

    @Override
    public void set(final EngineReplicationLangBytes engineReplicationLangBytes) {
        this.engineReplicationLang = engineReplicationLangBytes;
    }


    public CMap2EngineReplicator(final RequestContext context) {
        this.context = context;
    }

    net.openhft.lang.io.Bytes toLangBytes(Bytes b) {
        return wrap(b.bytes().address(), b.remaining());
    }

    public void put(final Bytes key, final Bytes value,
                    final byte remoteIdentifier,
                    final long timestamp) {
        engineReplicationLang.put(toLangBytes(key), toLangBytes(value), remoteIdentifier,
                timestamp);
    }

    private void remove(final Bytes key, final byte remoteIdentifier, final long timestamp) {
        engineReplicationLang.remove(toLangBytes(key), remoteIdentifier, timestamp);
    }

    @Override
    public byte identifier() {

        return engineReplicationLang.identifier();
    }

    private void put(@NotNull final ReplicationEntry entry) {
        put(entry.key(), entry.value(), entry.identifier(), entry.timestamp());
    }

    private void remove(@NotNull final ReplicationEntry entry) {
        remove(entry.key(), entry.identifier(), entry.timestamp());
    }

    @Override
    public void applyReplication(@NotNull final ReplicationEntry entry) {
        if (entry.isDeleted())
            remove(entry);
        else
            put(entry);
    }



    public static class VanillaReplicatedEntry implements ReplicationEntry {

        private final Bytes key;
        private final Bytes value;
        private final long timestamp;
        private final byte identifier;
        private final boolean isDeleted;
        private final long bootStrapTimeStamp;

        /**
         * @param key                the key of the entry
         * @param value              the value of the entry
         * @param identifier         the identifier of the remote server
         * @param timestamp          the timestamp send from the remote server, this time stamp was
         *                           the time the entry was removed
         * @param bootStrapTimeStamp sent to the client on every update this is the timestamp that
         *                           the remote client should bootstrap from when there has been a
         *                           disconnection, this time maybe later than the message time as
         *                           event are not send in chronological order from the bit set.
         */
        VanillaReplicatedEntry(final Bytes key,
                               final Bytes value,
                               final long timestamp,
                               final byte identifier,
                               final boolean isDeleted,
                               final long bootStrapTimeStamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
            this.identifier = identifier;
            this.isDeleted = isDeleted;
            this.bootStrapTimeStamp = bootStrapTimeStamp;
        }

        @Override
        public Bytes key() {
            return key;
        }

        @Override
        public Bytes value() {
            return value;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public byte identifier() {
            return identifier;
        }

        @Override
        public boolean isDeleted() {
            return isDeleted;
        }

        @Override
        public long bootStrapTimeStamp() {
            return bootStrapTimeStamp;
        }

    }

    @Override
    public void forEach(byte id, @NotNull Consumer<ReplicationEntry> consumer) throws InterruptedException {
        acquireModificationIterator(id).forEach(id, consumer);
    }

    @Override
    public ModificationIterator acquireModificationIterator(final byte remoteIdentifier) {
        final EngineModificationIterator instance = engineReplicationLang
                .acquireEngineModificationIterator(remoteIdentifier);

        return new ModificationIterator() {

            @Override
            public void forEach(byte id, @NotNull Consumer<ReplicationEntry> consumer) throws InterruptedException {

                while (hasNext()) {

                    nextEntry(entry -> {
                        consumer.accept(entry);
                        return true;
                    });

                }

            }


            @Override
            public boolean hasNext() {
                return instance.hasNext();
            }

            @Override
            public boolean nextEntry(@NotNull final EntryCallback callback) throws InterruptedException {
                return instance.nextEntry((key, value, timestamp,
                                           identifier, isDeleted,
                                           bootStrapTimeStamp) ->

                        callback.onEntry(new VanillaReplicatedEntry(
                                toKey(key),
                                toValue(value),
                                timestamp,
                                identifier,
                                isDeleted,
                                bootStrapTimeStamp)));
            }

            private Bytes<Void> toKey(final @NotNull net.openhft.lang.io.Bytes key) {
                final PointerBytesStore result = keyLocal.get();
                result.set(key.address(), key.capacity());
                return result.bytes();
            }

            private Bytes<Void> toValue(final @Nullable net.openhft.lang.io.Bytes value) {
                if (value == null)
                    return null;
                final PointerBytesStore result = valueLocal.get();
                result.set(value.address(), value.capacity());
                return result.bytes();
            }

            @Override
            public void dirtyEntries(final long fromTimeStamp) throws InterruptedException {
                instance.dirtyEntries(fromTimeStamp);
            }

            @Override
            public void setModificationNotifier(@NotNull final ModificationNotifier modificationNotifier) {
                instance.setModificationNotifier(modificationNotifier::onChange);
            }
        };

    }

    @Override
    public long lastModificationTime(final byte remoteIdentifier) {
        return engineReplicationLang.lastModificationTime(remoteIdentifier);
    }

    @Override
    public void setLastModificationTime(final byte identifier, final long timestamp) {
        engineReplicationLang.setLastModificationTime(identifier, timestamp);
    }

    @Override
    public void close() throws IOException {
        engineReplicationLang.close();
    }

}
