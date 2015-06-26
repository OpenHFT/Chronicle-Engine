/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.hash.replication.EngineReplicationLangBytesConsumer;
import net.openhft.chronicle.map.EngineReplicationLangBytes;
import net.openhft.chronicle.map.EngineReplicationLangBytes.EngineModificationIterator;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.IByteBufferBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static java.lang.ThreadLocal.withInitial;
import static net.openhft.lang.io.NativeBytes.wrap;

/**
 * Created by Rob Austin
 */
public class CMap2EngineReplicator implements EngineReplication,
        EngineReplicationLangBytesConsumer, View {

    private final RequestContext context;
    private final ThreadLocal<PointerBytesStore> keyLocal = withInitial(PointerBytesStore::new);
    private final ThreadLocal<PointerBytesStore> valueLocal = withInitial(PointerBytesStore::new);
    private EngineReplicationLangBytes engineReplicationLang;

    public CMap2EngineReplicator(RequestContext requestContext, Asset asset) {
        this(requestContext);
        asset.addView(EngineReplicationLangBytesConsumer.class, this);
    }

    public CMap2EngineReplicator(final RequestContext context) {
        this.context = context;
    }

    @Override
    public void set(final EngineReplicationLangBytes engineReplicationLangBytes) {
        this.engineReplicationLang = engineReplicationLangBytes;
    }

    net.openhft.lang.io.Bytes toLangBytes(BytesStore b) {
        if (b.underlyingObject() == null)
            return wrap(b.address(), b.readRemaining());
        else {
            ByteBuffer buffer = (ByteBuffer) b.underlyingObject();
            IByteBufferBytes wrap = ByteBufferBytes.wrap(buffer);
            wrap.limit((int) b.readLimit());
            return wrap;
        }
    }

    public void put(final BytesStore key, final BytesStore value,
                    final byte remoteIdentifier,
                    final long timestamp) {
        engineReplicationLang.put(toLangBytes(key), toLangBytes(value), remoteIdentifier, timestamp);
    }

    private void remove(final BytesStore key, final byte remoteIdentifier, final long timestamp) {
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

        setLastModificationTime(entry.identifier(), entry.bootStrapTimeStamp());
    }

    @Override
    public ModificationIterator acquireModificationIterator(final byte remoteIdentifier) {
        final EngineModificationIterator instance = engineReplicationLang
                .acquireEngineModificationIterator(remoteIdentifier);

        return new ModificationIterator() {
            @Override
            public void forEach(@NotNull Consumer<ReplicationEntry> consumer) throws InterruptedException {
                while (hasNext()) {
                    nextEntry(entry -> {
                        consumer.accept(entry);
                        return true;
                    });
                }
            }

            public boolean hasNext() {
                return instance.hasNext();
            }

            private boolean nextEntry(@NotNull final EntryCallback callback) throws InterruptedException {
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
                Bytes<Void> voidBytes = result.bytesForRead();
                return voidBytes;
            }

            private Bytes<Void> toValue(final @Nullable net.openhft.lang.io.Bytes value) {
                if (value == null)
                    return null;
                final PointerBytesStore result = valueLocal.get();
                result.set(value.address(), value.capacity());
                Bytes<Void> voidBytes = result.bytesForRead();
                return voidBytes;
            }

            @Override
            public void dirtyEntries(final long fromTimeStamp) throws InterruptedException {
                instance.dirtyEntries(fromTimeStamp);
            }

            @Override
            public void setModificationNotifier(@NotNull final ModificationNotifier modificationNotifier) {
                instance.setModificationNotifier(new EngineReplicationLangBytes.EngineReplicationModificationNotifier() {

                    @Override
                    public void onChange() {
                        modificationNotifier.onChange();
                    }
                });
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
    public String toString() {
        return "CMap2EngineReplicator{" +
                "context=" + context +
                ", identifier=" + engineReplicationLang.identifier() +
                ", keyLocal=" + keyLocal +
                ", valueLocal=" + valueLocal +
                '}';
    }

    public static class VanillaReplicatedEntry implements ReplicationEntry {

        private BytesStore key;
        private BytesStore value;
        private long timestamp;
        private byte identifier;
        private boolean isDeleted;
        private long bootStrapTimeStamp;

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
        VanillaReplicatedEntry(final BytesStore key,
                               final BytesStore value,
                               final long timestamp,
                               final byte identifier,
                               final boolean isDeleted,
                               final long bootStrapTimeStamp) {
            this.key = key;
            // must be native
            key.address();
            this.value = value;
            // must be native
            value.address();
            this.timestamp = timestamp;
            this.identifier = identifier;
            this.isDeleted = isDeleted;
            this.bootStrapTimeStamp = bootStrapTimeStamp;
        }

        @Override
        public BytesStore key() {
            return key;
        }

        @Override
        public BytesStore value() {
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

        @Override
        public void key(BytesStore key) {
            this.key = key;
        }

        @Override
        public void value(BytesStore value) {
            this.value = value;
        }

        @Override
        public void timestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public void identifier(byte identifier) {
            this.identifier = identifier;
        }

        @Override
        public void isDeleted(boolean isDeleted) {
            this.isDeleted = isDeleted;
        }

        @Override
        public void bootStrapTimeStamp(long bootStrapTimeStamp) {
            this.bootStrapTimeStamp = bootStrapTimeStamp;
        }

    }
}
