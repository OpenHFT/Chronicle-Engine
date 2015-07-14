package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

/**
 * Created by Rob Austin
 */
public class AbstractHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHandler.class);
    @Nullable
    WireOut outWire = null;
    protected volatile boolean connectionClosed = false;

    void setOutWire(final WireOut outWire) {
        this.outWire = outWire;
    }

    /**
     * write and exceptions and rolls back if no data was written
     */
    void writeData(@NotNull Bytes inBytes, @NotNull Consumer<WireOut> c) {
        outWire.writeDocument(false, out -> {
            final long readPosition = inBytes.readPosition();
            final long position = outWire.bytes().writePosition();
            try {
                c.accept(outWire);
            } catch (Throwable t) {
                inBytes.readPosition(readPosition);
                LOG.info("While reading " + inBytes.toDebugString(), " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName(() -> "exception").throwable(t);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().writePosition()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        });

        if (Jvm.IS_DEBUG && YamlLogging.showServerWrites)
            try {
                System.out.println("\nServer Sends:\n" +
                        Wires.fromSizePrefixedBlobs(outWire.bytes(), outWire.bytes().readPosition(), outWire
                                .bytes().readRemaining()));
            } catch (Exception e) {
                System.out.println("\nServer Sends ( corrupted ) :\n" +
                        outWire.bytes().toDebugString());
            }

        System.out.println("");


    }

    public static void nullCheck(@Nullable Object o) {
        if (o == null)
            throw new NullPointerException();
    }

    public void onEndOfConnection(boolean heartbeatTimeOut) {
        connectionClosed = true;
    }
}
