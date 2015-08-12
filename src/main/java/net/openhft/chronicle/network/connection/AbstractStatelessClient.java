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

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ConnectionDroppedException;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.Time;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public abstract class AbstractStatelessClient<E extends ParameterizeWireKey> implements Closeable {

    @NotNull
    protected final TcpChannelHub hub;
    private final long cid;
    protected final String csp;

    /**
     * @param hub for this connection
     * @param cid used by proxies such as the entry-set
     * @param csp the uri of the request
     */
    protected AbstractStatelessClient(@NotNull final TcpChannelHub hub,
                                      long cid,
                                      @NotNull final String csp) {
        this.cid = cid;
        this.csp = csp;
        this.hub = hub;
    }

    protected static <E extends ParameterizeWireKey>
    Consumer<ValueOut> toParameters(@NotNull final E eventId,
                                    @Nullable final Object... args) {
        return out -> {
            final WireKey[] paramNames = eventId.params();

            //args can be null, e.g. when get() is called from Reference.
            if (args == null) return;

            assert args.length == paramNames.length :
                    "methodName=" + eventId +
                            ", args.length=" + args.length +
                            ", paramNames.length=" + paramNames.length;

            if (paramNames.length == 1) {
                out.object(args[0]);
                return;
            }

            out.marshallable(m -> {

                for (int i = 0; i < paramNames.length; i++) {
                    final ValueOut vo = m.write(paramNames[i]);
                    vo.object(args[i]);
                }

            });
        };
    }


    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @Nullable R usingValue,
            @NotNull final Class<R> resultType,
            @NotNull Object... args) {

        Function<ValueIn, R> consumerIn = resultType == CharSequence.class && usingValue != null
                ? f -> {
            f.textTo((StringBuilder) usingValue);
            return usingValue;
        }
                : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                toParameters(eventId, args),
                consumerIn);
    }


    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @Nullable R usingValue,
            @NotNull final Class<R> resultType) {

        Function<ValueIn, R> consumerIn = resultType == CharSequence.class && usingValue != null
                ? f -> {
            f.textTo((StringBuilder) usingValue);
            return usingValue;
        }
                : f -> f.object(resultType);
        return proxyReturnWireConsumerInOut(eventId,
                CoreFields.reply,
                x -> {
                },
                consumerIn);
    }

    /**
     * this method will re attempt a number of times until successful,if connection is dropped to
     * the  remote server the TcpChannelHub may ( if configured )  automatically failover to another
     * host.
     *
     * @param s   the supply
     * @param <T> the type of supply
     * @return the result for s.get()
     */
    protected <T> T attempt(@NotNull final Supplier<T> s) {

        ConnectionDroppedException t = null;
        for (int i = 0; i < 10; i++) {

            try {
                return s.get();
            } catch (ConnectionDroppedException e) {
                t = e;
            }
            // pause then resend the request
            Jvm.pause(200);
        }

        throw t;
    }

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::int64);
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::int32);
    }

    protected byte proxyReturnByte(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::int8);
    }

    protected byte proxyReturnByte(@NotNull WireKey reply, @NotNull final WireKey eventId) {
        return proxyReturnWireConsumerInOut(eventId, reply, null, ValueIn::int8);
    }

    protected int proxyReturnUint16(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, ValueIn::uint16);
    }

    protected <T> T proxyReturnWireConsumer(@NotNull final WireKey eventId,
                                            @NotNull final Function<ValueIn, T> consumer) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readWire(sendEvent(startTime, eventId, null), startTime, CoreFields
                .reply, consumer));
    }

    protected <T> T proxyReturnWireConsumerInOut(@NotNull final WireKey eventId,
                                                 @NotNull final WireKey reply,
                                                 @Nullable final Consumer<ValueOut> consumerOut,
                                                 @NotNull final Function<ValueIn, T> consumerIn) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readWire(sendEvent(startTime, eventId, consumerOut), startTime,
                reply, consumerIn));
    }

    @SuppressWarnings("SameParameterValue")
    private void proxyReturnVoid(@NotNull final WireKey eventId,
                                 @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = Time.currentTimeMillis();

        attempt(() -> readWire(sendEvent(startTime, eventId, consumer), startTime, CoreFields
                .reply, v -> v.marshallable(ReadMarshallable.DISCARD)));
    }

    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId) {
        proxyReturnVoid(eventId, null);
    }

    protected long sendEvent(final long startTime,
                             @NotNull final WireKey eventId,
                             @Nullable final Consumer<ValueOut> consumer) {
        long tid;
        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");
        try {
            hub.checkConnection();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // can't use lock() as we are setting tid.
        hub.outBytesLock().lock();
        try {
            tid = writeMetaDataStartTime(startTime);
            hub.outWire().writeDocument(false, wireOut -> {

                final ValueOut valueOut = wireOut.writeEventName(eventId);

                if (consumer == null)
                    valueOut.marshallable(WriteMarshallable.EMPTY);
                else
                    consumer.accept(valueOut);
            });

            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }
        return tid;
    }

    /**
     * @param eventId              the wire event id
     * @param consumer             a function consume the wire
     * @param reattemptUponFailure if false - will only be sent if the connection is valid
     */
    protected boolean sendEventAsync(@NotNull final WireKey eventId,
                                     @Nullable final Consumer<ValueOut> consumer,
                                     boolean reattemptUponFailure) {

        if (reattemptUponFailure)
            try {
                hub.checkConnection();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        else if (!hub.isOpen())
            return false;

        if (!reattemptUponFailure) {

            hub.lock(() -> {
                try {
                    sendEventAsyncWithoutLock(eventId, consumer);
                } catch (IORuntimeException e) {
                    // this can occur if the socket is not currently connected
                }
            });
            return true;
        }

        attempt(() -> {
            hub.lock(() -> {
                try {
                    sendEventAsyncWithoutLock(eventId, consumer);
                } catch (IORuntimeException e) {
                    // this can occur if the socket is not currently connected
                }
            });
            return true;
        });

        return false;
    }


    protected void sendEventAsyncWithoutLock(@NotNull final WireKey eventId,
                                             @Nullable final Consumer<ValueOut> consumer) {

        writeAsyncMetaData();
        hub.outWire().writeDocument(false, wireOut -> {
            final ValueOut valueOut = wireOut.writeEventName(eventId);
            if (consumer == null)
                valueOut.marshallable(WriteMarshallable.EMPTY);
            else
                consumer.accept(valueOut);
        });

        hub.writeSocket(hub.outWire());
    }

    /**
     * @param startTime the start time of this transaction
     * @return the translation id ( which is sent to the server )
     */
    private long writeMetaDataStartTime(long startTime) {
        return hub.writeMetaDataStartTime(startTime, hub.outWire(), csp, cid);
    }

    /**
     * Useful for when you know the tid
     *
     * @param tid the tid transaction
     */
    protected void writeMetaDataForKnownTID(long tid) {
        hub.writeMetaDataForKnownTID(tid, hub.outWire(), csp, cid);
    }

    /**
     * if async meta data is written, no response will be returned from the server
     */
    private void writeAsyncMetaData() {
        hub.writeAsyncHeader(hub.outWire(), csp, cid);
    }

    private void checkIsData(@NotNull Wire wireIn) {
        Bytes<?> bytes = wireIn.bytes();
        int dataLen = bytes.readVolatileInt();

        if (!Wires.isData(dataLen))
            throw new IllegalStateException("expecting a data blob, from ->" + Bytes.toString
                    (bytes, 0, bytes.readLimit()));
    }

    protected boolean readBoolean(long tid, long startTime) throws ConnectionDroppedException {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        final Wire wireIn = hub.proxyReply(timeoutTime, tid);
        checkIsData(wireIn);

        return readReply(wireIn, CoreFields.reply, ValueIn::bool);

    }

    private <R> R readReply(@NotNull WireIn wireIn, @NotNull WireKey replyId, @NotNull Function<ValueIn, R> function) {

        StringBuilder eventName = Wires.acquireStringBuilder();
        final ValueIn event = wireIn.read(eventName);

        if (replyId.contentEquals(eventName))
            return function.apply(event);

        if (CoreFields.exception.contentEquals(eventName)) {
            throw Jvm.rethrow(event.throwable(true));
        }

        throw new UnsupportedOperationException("unknown event=" + eventName);
    }


    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBooleanWithArgs(
            @NotNull final E eventId,
            @NotNull final Object... args) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readBoolean(sendEvent(startTime, eventId, toParameters(eventId, args)
        ), startTime));
    }

    protected boolean proxyReturnBooleanWithSequence(
            @NotNull final E eventId,
            @NotNull final Collection sequence) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readBoolean(sendEvent(startTime, eventId, out -> sequence.forEach
                (out::object)), startTime));
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBoolean(@NotNull final WireKey eventId) {
        final long startTime = Time.currentTimeMillis();
        return attempt(() -> readBoolean(sendEvent(startTime, eventId, null), startTime));
    }

    private <T> T readWire(long tid, long startTime, @NotNull WireKey reply, @NotNull Function<ValueIn, T> c) throws ConnectionDroppedException {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive

        final Wire wire = hub.proxyReply(timeoutTime, tid);
        checkIsData(wire);
        return readReply(wire, reply, c);

    }

    protected int readInt(long tid, long startTime) throws ConnectionDroppedException {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        final Wire wireIn = hub.proxyReply(timeoutTime, tid);
        checkIsData(wireIn);
        return wireIn.read(reply).int32();

    }

    @Override
    public void close() {
        hub.close();
    }

}
