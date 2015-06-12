package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.hash.replication.EngineReplicationLangBytesConsumer;
import net.openhft.chronicle.map.EngineReplicationLangBytes;
import net.openhft.chronicle.map.EngineReplicationLangBytes.EngineModificationIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.lang.io.NativeBytes.wrap;

/**
 * Created by Rob Austin
 */
public class EngineReplicator implements EngineReplication, EngineReplicationLangBytesConsumer {

    private final RequestContext context;
    private EngineReplicationLangBytes engineReplicationLang;
    private final ThreadLocal<PointerBytesStore> keyLocal = withInitial(PointerBytesStore::new);
    private final ThreadLocal<PointerBytesStore> valueLocal = withInitial(PointerBytesStore::new);

    @Override
    public void set(final EngineReplicationLangBytes engineReplicationLangBytes) {
        this.engineReplicationLang = engineReplicationLangBytes;
    }

    public EngineReplicator(final RequestContext context) {
        this.context = context;
    }

    net.openhft.lang.io.Bytes toLangBytes(Bytes b) {
        return wrap(b.bytes().address(), b.remaining());
    }

    @Override
    public void put(final Bytes key, final Bytes value, final byte remoteIdentifier, final long timestamp) {
        engineReplicationLang.put(toLangBytes(key), toLangBytes(value), remoteIdentifier, timestamp);
    }

    @Override
    public void remove(final Bytes key, final byte remoteIdentifier, final long timestamp) {
        engineReplicationLang.remove(toLangBytes(key), remoteIdentifier, timestamp);
    }

    @Override
    public byte identifier() {
        return engineReplicationLang.identifier();
    }

    @Override
    public ModificationIterator acquireModificationIterator(final byte remoteIdentifier) {
        final EngineModificationIterator instance = engineReplicationLang
                .acquireEngineModificationIterator(remoteIdentifier);

        return new ModificationIterator() {

            @Override
            public boolean hasNext() {
                return instance.hasNext();
            }

            @Override
            public boolean nextEntry(@NotNull final EntryCallback callback) throws InterruptedException {
                return instance.nextEntry((key, value, timestamp,
                                           identifier, isDeleted,
                                           bootStrapTimeStamp) ->
                        callback.onEntry(toKey(key), toValue(value), timestamp,
                                identifier, isDeleted, bootStrapTimeStamp));
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
