package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static net.openhft.chronicle.wire.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public abstract class AbstractStatelessClient<E extends ParameterizeWireKey> {

    @NotNull
    protected final TcpConnectionHub hub;
    private final long cid;
    protected String csp;

    /**
     * @param hub
     * @param cid used by proxies such as the entry-set
     * @param csp
     */
    public AbstractStatelessClient(@NotNull final TcpConnectionHub hub,
                                   long cid,
                                   @NotNull final String csp) {
        this.cid = cid;
        this.csp = csp;
        this.hub = hub;
    }

    @Nullable
    protected <R> R proxyReturnTypedObject(
            @NotNull final E eventId,
            @Nullable R usingValue,
            @NotNull final Class<R> resultType,
            @Nullable Object... args) {

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

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int64());
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int32());
    }

    protected byte proxyReturnByte(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int8());
    }

    protected int proxyReturnUint16(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.uint16());
    }

    public <T> T proxyReturnWireConsumer(@NotNull final WireKey eventId,
                                         @NotNull final Function<ValueIn, T> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, null);
        return readWire(tid, startTime, CoreFields.reply, consumer);
    }

    public <T> T proxyReturnWireConsumerInOut(@NotNull final WireKey eventId,
                                              @NotNull final CoreFields reply,
                                              @Nullable final Consumer<ValueOut> consumerOut,
                                              @NotNull final Function<ValueIn, T> consumerIn) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumerOut);
        return readWire(tid, startTime, reply, consumerIn);
    }

    public <T> T proxyReturnWireConsumerInOut(@NotNull final WireKey eventId,
                                              @NotNull final CoreFields reply,
                                              @Nullable final Consumer<ValueOut> consumerOut,
                                              @NotNull final Function<ValueIn, T> consumerIn,
                                              @Nullable final LongConsumer tidConsumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumerOut);
        tidConsumer.accept(tid);
        return readWire(tid, startTime, reply, consumerIn);
    }



    @SuppressWarnings("SameParameterValue")
    protected void proxyReturnVoid(@NotNull final WireKey eventId,
                                   @Nullable final Consumer<ValueOut> consumer) {
        final long startTime = System.currentTimeMillis();
        long tid = sendEvent(startTime, eventId, consumer);
        readWire(tid, startTime, CoreFields.reply, v -> v.marshallable(ReadMarshallable.DISCARD));
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

    protected void sendEventAsync(@NotNull final WireKey eventId,
                                  @Nullable final Consumer<ValueOut> consumer) {
        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.outBytesLock().lock();
        try {

            writeAsyncMetaData(System.currentTimeMillis());
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
    }

    /**
     * @param startTime the start time of this transaction
     * @return the translation id ( which is sent to the server )
     */
    protected long writeMetaDataStartTime(long startTime) {
        return hub.writeMetaDataStartTime(startTime, hub.outWire(), csp, cid);
    }

    /**
     * Useful for when you know the tid
     * @param tid the tid transaction
     */
    protected void writeMetaDataForKnownTID(long tid) {
        hub.writeMetaDataForKnownTID(tid, hub.outWire(), csp, cid);
    }

    /**
     * if async meta data is written, no response will be returned from the server
     *
     * @param startTime the start time of this transaction
     */
    protected void writeAsyncMetaData(long startTime) {
        hub.startTime(startTime);
        hub.writeAsyncHeader(hub.outWire(), csp, cid);
    }

    protected void checkIsData(@NotNull Wire wireIn) {
        int datalen = wireIn.bytes().readVolatileInt();

        if (!Wires.isData(datalen))
            throw new IllegalStateException("expecting a data blob, from ->" + Bytes.toDebugString
                    (wireIn.bytes(), 0, wireIn.bytes().limit()));
    }

    @NotNull
    StringBuilder eventName = new StringBuilder();

    protected boolean readBoolean(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, tid);
            checkIsData(wireIn);

            return readReply(wireIn, CoreFields.reply, v -> v.bool());
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    protected <R> R readReply(@NotNull WireIn wireIn, @NotNull WireKey replyId, @NotNull Function<ValueIn, R> function) {
        final ValueIn event = wireIn.read(eventName);

        if (replyId.contentEquals(eventName))
            return function.apply(event);

        if (CoreFields.exception.contentEquals(eventName)) {
            throw Jvm.rethrow(event.throwable(true));
        }

        throw new UnsupportedOperationException("unknown event=" + eventName);
    }

    protected void readReplyConsumer(@NotNull WireIn wireIn, @NotNull WireKey replyId, @NotNull Consumer<ValueIn> consumer) {
        final ValueIn event = wireIn.read(eventName);

        if (replyId.contentEquals(eventName)) {
            consumer.accept(event);
            return;
        }

        if (CoreFields.exception.contentEquals(eventName)) {
            throw Jvm.rethrow(event.throwable(true));
        }

        throw new UnsupportedOperationException("unknown event=" + eventName);
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBooleanWithArgs(
            @NotNull final E eventId,
            @NotNull final Object... args) {
        final long startTime = System.currentTimeMillis();

        final long tid = sendEvent(startTime, eventId, toParameters(eventId, args));
        return readBoolean(tid, startTime);
    }

    protected boolean proxyReturnBooleanWithSequence(
            @NotNull final E eventId,
            @NotNull final Collection sequence) {
        final long startTime = System.currentTimeMillis();

        final long tid = sendEvent(startTime, eventId, out -> sequence.forEach(out::object));
        return readBoolean(tid, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    protected boolean proxyReturnBoolean(@NotNull final WireKey eventId) {
        final long startTime = System.currentTimeMillis();
        final long tid = sendEvent(startTime, eventId, null);
        return readBoolean(tid, startTime);
    }

    private <T> T readWire(long tid, long startTime, @NotNull WireKey reply, @NotNull Function<ValueIn, T> c) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wire = hub.proxyReply(timeoutTime, tid);
            checkIsData(wire);
            return readReply(wire, reply, c);
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    public static <E extends ParameterizeWireKey>
    Consumer<ValueOut> toParameters(@NotNull final E eventId,
                                    @Nullable final Object... args) {
        return out -> {
            final WireKey[] paramNames = eventId.params();

            assert args != null;
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

    protected int readInt(long tid, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, tid);
            checkIsData(wireIn);
            return wireIn.read(reply).int32();
        } finally {
            hub.inBytesLock().unlock();
        }
    }
}
