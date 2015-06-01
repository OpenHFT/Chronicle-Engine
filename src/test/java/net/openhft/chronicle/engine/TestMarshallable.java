package net.openhft.chronicle.engine;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by daniel on 08/04/15.
 */
public class TestMarshallable implements Marshallable {
    private StringBuilder name = new StringBuilder();
    private int count;

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(() -> "name").textTo(name);
        count = wire.read(()->"count").int32();
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "name").text(name);
        wire.write(()->"count").int32(count);
    }

    public StringBuilder getName() {
        return name;
    }

    public void setName(StringBuilder name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
