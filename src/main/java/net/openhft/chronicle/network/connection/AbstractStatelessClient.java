package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.wire.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public abstract class AbstractStatelessClient<E extends ParameterizeWireKey> {

    protected final TcpConnectionHub hub;
    private final long cid;
    protected final String channelName;
    protected String csp;

    /**
     * @param channelName
     * @param hub
     * @param cid         used by proxies such as the entry-set
     * @param csp
     */
    public AbstractStatelessClient(@NotNull final String channelName,
                                   @NotNull final TcpConnectionHub hub,
                                   long cid,
                                   String csp) {
        this.cid = cid;
        this.csp = csp;
        this.hub = hub;
        this.channelName = channelName;
    }

    @SuppressWarnings("SameParameterValue")
    protected long proxyReturnLong(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int64());
    }

    @SuppressWarnings("SameParameterValue")
    protected int proxyReturnInt(@NotNull final WireKey eventId) {
        return proxyReturnWireConsumer(eventId, f -> f.int32());
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
            tid = writeMetaData(startTime);
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
    protected long writeMetaData(long startTime) {
        return hub.writeMetaData(startTime, hub.outWire(), csp, cid);
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

    protected void checkIsData(Wire wireIn) {
        int datalen = wireIn.bytes().readVolatileInt();

        if (!Wires.isData(datalen))
            throw new IllegalStateException("expecting a data blob, from ->" + Bytes.toDebugString
                    (wireIn.bytes(), 0, wireIn.bytes().limit()));
    }

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

    <R> R readReply(WireIn wireIn, WireKey replyId, Function<ValueIn, R> function) {
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

    private <T> T readWire(long tid, long startTime, WireKey reply, Function<ValueIn, T> c) {
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

    @SuppressWarnings("UnusedDeclaration")
    void identifier(int localIdentifier) {
        hub.localIdentifier = localIdentifier;
    }
}
