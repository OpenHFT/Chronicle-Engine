/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import net.openhft.chronicle.wire.WriteMarshallable;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.WireOutPublisher.newThrottledWireOutPublisher;
import static net.openhft.chronicle.wire.WriteMarshallable.EMPTY;

/**
 * Created by Rob Austin
 */
abstract class AbstractHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHandler.class);
    @Nullable
    WireOut outWire = null;
    volatile boolean connectionClosed = false;
    RequestContext requestContext;

    static void nullCheck(@Nullable Object o) {
        if (o == null)
            throw new NullPointerException();
    }

    void setOutWire(@NotNull final WireOut outWire) {
        this.outWire = outWire;
    }

    /**
     * write and exceptions and rolls back if no data was written
     */
    void writeData(@NotNull Bytes inBytes, @NotNull WriteMarshallable c) {
        outWire.writeDocument(false, out -> {
            final long readPosition = inBytes.readPosition();
            final long position = outWire.bytes().writePosition();
            try {
                c.writeMarshallable(outWire);

            } catch (Throwable t) {
                inBytes.readPosition(readPosition);
                if (LOG.isInfoEnabled())
                    LOG.info("While reading " + inBytes.toDebugString(),
                            " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName(() -> "exception").throwable(t);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().writePosition()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        });

        logYaml();
    }

    /**
     * write and exceptions and rolls back if no data was written
     */
    void writeData(boolean isNotComplete, @NotNull Bytes inBytes, @NotNull WriteMarshallable c) {

        final WriteMarshallable marshallable = out -> {
            final long readPosition = inBytes.readPosition();
            final long position = outWire.bytes().writePosition();
            try {
                c.writeMarshallable(outWire);

            } catch (Throwable t) {
                inBytes.readPosition(readPosition);
                if (LOG.isInfoEnabled())
                    LOG.info("While reading " + inBytes.toDebugString(),
                            " processing wire " + c, t);
                outWire.bytes().writePosition(position);
                outWire.writeEventName(() -> "exception").throwable(t);
            }

            // write 'reply : {} ' if no data was sent
            if (position == outWire.bytes().writePosition()) {
                outWire.writeEventName(reply).marshallable(EMPTY);
            }
        };

        if (isNotComplete)
            outWire.writeNotCompleteDocument(false, marshallable);
        else
            outWire.writeDocument(false, marshallable);

        logYaml();
    }

    void logYaml() {
        if (YamlLogging.showServerWrites())
            try {
                assert outWire.startUse();
                LOG.info("\nServer Sends:\n" +
                        Wires.fromSizePrefixedBlobs(outWire.bytes()));

            } catch (Exception e) {
                Jvm.warn().on(getClass(), "\nServer Sends ( corrupted ) :\n" +
                        outWire.bytes().toDebugString());
            } finally {
                assert outWire.endUse();
            }
    }

    /**
     * called when the connection is closed
     */
    public void onEndOfConnection(boolean heartbeatTimeOut) {
        connectionClosed = true;
        unregisterAll();
    }

    /**
     * called when the connection is closed
     */
    protected void unregisterAll() {

    }

    /**
     * @param publisher
     * @return If the throttlePeriodMs is set returns a throttled wire out publisher, otherwise the
     * origional
     */
    WireOutPublisher publisher(final WireOutPublisher publisher) {
        return requestContext.throttlePeriodMs() == 0 ?
                publisher :
                newThrottledWireOutPublisher(requestContext.throttlePeriodMs(), publisher);
    }
}
