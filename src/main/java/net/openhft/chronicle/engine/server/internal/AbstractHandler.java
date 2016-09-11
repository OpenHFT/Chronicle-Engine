/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
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
                        Wires.fromSizePrefixedBlobs((Wire) outWire));

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
    void onEndOfConnection() {
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
