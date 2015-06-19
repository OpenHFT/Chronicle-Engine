package net.openhft.chronicle.engine.map.replication;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by Rob Austin
 */
public class Bootstrap implements Marshallable {

    private byte identifier;

    private long lastUpdatedTime;

    public long lastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void lastUpdatedTime(final long lastModificationTime) {
        this.lastUpdatedTime = lastModificationTime;
    }

    public void identifier(final byte identifier) {
        this.identifier = identifier;
    }

    public byte identifier() {
        return identifier;
    }

    @Override
    public void writeMarshallable(final WireOut wire) {
        wire.write(() -> "id").int8(identifier);
    }

    @Override
    public void readMarshallable(final WireIn wire) throws IllegalStateException {
        wire.read(() -> "id").int8();
    }
}
