package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

public class MyMarshallable implements Marshallable {

    String s;

    public MyMarshallable(String s) {
        this.s = s;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        s = wire.read().text();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write().text(s);
    }

    @Override
    public String toString() {
        return s;
    }
}