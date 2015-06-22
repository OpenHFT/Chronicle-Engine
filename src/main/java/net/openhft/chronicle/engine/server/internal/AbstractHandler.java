package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

import static net.openhft.chronicle.wire.CoreFields.reply;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

/**
 * Created by Rob Austin
 */
public class AbstractHandler {

    Wire outWire = null;

    void setOutWire(final Wire outWire) {
        this.outWire = outWire;
    }

    /**
     * write and exceptions and rolls back if no data was written
     */
    void writeData(@NotNull Consumer<WireOut> c) {
        outWire.writeDocument(false, out -> {

            final long position = outWire.bytes().writePosition();
            try {
                c.accept(outWire);
            } catch (Exception exception) {
                outWire.bytes().writePosition(position);
                outWire.writeEventName(() -> "exception").throwable(exception);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().writePosition()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        });

        if (YamlLogging.showServerWrites)
            try {
                System.out.println("server-writes:\n" +
                        Wires.fromSizePrefixedBlobs(outWire.bytes(), 0, outWire.bytes().writePosition()));
            } catch (Exception e) {
                System.out.println("server-writes:\n" +
                        outWire.bytes().toDebugString());
            }
    }
}
