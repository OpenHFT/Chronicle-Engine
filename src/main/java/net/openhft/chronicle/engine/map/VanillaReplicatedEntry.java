package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.PointerBytesStore;
import net.openhft.chronicle.engine.api.EngineReplication;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class VanillaReplicatedEntry implements EngineReplication.ReplicationEntry {

    private final byte remoteIdentifier;
    private BytesStore key;
    @Nullable
    private BytesStore value;
    private long timestamp;
    private byte identifier;
    private boolean isDeleted;
    private long bootStrapTimeStamp;

    /**
     * @param key                the key of the entry
     * @param value              the value of the entry
     * @param timestamp          the timestamp send from the remote server, this time stamp was
     *                           the time the entry was removed
     * @param identifier         the identifier of the remote server
     * @param bootStrapTimeStamp sent to the client on every update this is the timestamp that
     *                           the remote client should bootstrap from when there has been a
     * @param remoteIdentifier   the identifier of the server we are sending data to ( only used
     *                           as a comment )
     */
    VanillaReplicatedEntry(@NotNull final BytesStore key,
                           @Nullable final BytesStore value,
                           final long timestamp,
                           final byte identifier,
                           final boolean isDeleted,
                           final long bootStrapTimeStamp,
                           byte remoteIdentifier) {
        this.key = key;
        this.remoteIdentifier = remoteIdentifier;
        // must be native
        assert key.underlyingObject() == null;
        this.value = value;
        // must be native
        assert value == null || value.underlyingObject() == null;
        this.timestamp = timestamp;
        this.identifier = identifier;
        this.isDeleted = isDeleted;
        this.bootStrapTimeStamp = bootStrapTimeStamp;
    }

    // for deserialization only.
    public VanillaReplicatedEntry() {
        remoteIdentifier = 0;
        key = BytesStore.nativePointer();
        value = BytesStore.nativePointer();
    }

    public void clear() {
        key.isPresent(false);
        value.isPresent(false);
        timestamp = 0;
        identifier = 0;
        isDeleted = false;
        bootStrapTimeStamp = 0;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) {
        wire.read(() -> "key").bytesSet((PointerBytesStore) key);
        wire.read(() -> "value").bytesSet((PointerBytesStore) value);
        timestamp(wire.read(() -> "timestamp").int64());
        identifier(wire.read(() -> "identifier").int8());
        isDeleted(wire.read(() -> "isDeleted").bool());
        bootStrapTimeStamp(wire.read(() -> "bootStrapTimeStamp").int64());
    }

    @Override
    public BytesStore key() {
        return key;
    }

    @Nullable
    @Override
    public BytesStore value() {
        return value != null && value.isPresent() ? value : null;
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
    public byte remoteIdentifier() {
        return remoteIdentifier;
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

    @NotNull
    @Override
    public String toString() {
        final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        new TextWire(bytes).writeDocument(false, d -> d.write().typedMarshallable(this));
        return "\n" + Wires.fromSizePrefixedBlobs(bytes);

    }
}
