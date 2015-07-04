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
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static java.lang.Thread.currentThread;
import static java.lang.ThreadLocal.withInitial;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.engine.server.internal.SystemHandler.EventId.*;


/**
 * Created by Rob Austin
 */
public class TcpChannelHub implements View, Closeable {

    public static final int HEATBEAT_PING_PERIOD = getInteger("heartbeat.ping.period", 5_000);
    public static final int HEATBEAT_TIMEOUT_PERIOD = getInteger("heartbeat.timeout", 10_000);

    public static final int SIZE_OF_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(TcpChannelHub.class);
    public final long timeoutMs;
    @NotNull
    protected final String name;
    @NotNull
    protected final InetSocketAddress remoteAddress;
    protected final int tcpBufferSize;
    final Wire outWire;
    final Wire inWire;
    private final ReentrantLock outBytesLock = new ReentrantLock();
    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);
    private final SessionProvider sessionProvider;
    @NotNull
    private final TcpSocketConsumer tcpSocketConsumer;
    private final EventLoop eventLoop;
    private final Function<Bytes, Wire> wire;

    private long largestChunkSoFar = 0;

    @Nullable
    private volatile SocketChannel clientChannel;

    private long limitOfLast = 0;

    // set up in the header
    private long startTime;
    private volatile boolean closed;
    private final Wire handShakingWire;
    private final String description;

    enum ConnecitonStatus {
        RECONNECT,
        DISCONNECTED,
        CONNECTED
    }


    private final AtomicReference<ConnecitonStatus> currentState = new AtomicReference<ConnecitonStatus>(null);
    private final AtomicReference<ConnecitonStatus> desiredState = new AtomicReference<ConnecitonStatus>();

    public TcpChannelHub(@NotNull SessionProvider sessionProvider,
                         @NotNull String description,
                         @NotNull EventLoop eventLoop,
                         @NotNull Function<Bytes, Wire> wire) {
        this.description = description;

        this.eventLoop = eventLoop;

        this.tcpBufferSize = 64 << 10;
        this.remoteAddress = TCPRegistry.lookup(description);


        this.outWire = wire.apply(elasticByteBuffer());
        this.inWire = wire.apply(elasticByteBuffer());
        this.name = " connected to " + remoteAddress.toString();
        this.timeoutMs = 10_000;
        this.wire = wire;
        Bytes<ByteBuffer> byteBufferBytes = Bytes.elasticByteBuffer();
        handShakingWire = wire.apply(byteBufferBytes);
        this.sessionProvider = sessionProvider;
        this.tcpSocketConsumer = new TcpSocketConsumer(wire, description);

    }

    @Nullable
    static SocketChannel openSocketChannel()
            throws IOException {
        SocketChannel result = SocketChannel.open();
        result.socket().setTcpNoDelay(true);
        return result;
    }

    static void logToStandardOutMessageReceived(@NotNull Wire wire) {
        Bytes<?> bytes = wire.bytes();

        if (!YamlLogging.clientReads)
            return;

        final long position = bytes.writePosition();
        final long limit = bytes.writeLimit();
        try {
            try {
                System.out.println("\nreceives:\n" +

                        ((wire instanceof TextWire) ?
                                "```yaml\n" +
                                        Wires.fromSizePrefixedBlobs(bytes) :
                                "```\n" +
//                                        Wires.fromSizePrefixedBlobs(bytes)
                                        BytesUtil.toHexString(bytes, bytes.readPosition(), bytes.readRemaining())

                        ) +
                        "```\n");
                YamlLogging.title = "";
                YamlLogging.writeMessage = "";
            } catch (Exception e) {

                String x = Bytes.toString(bytes);
                System.out.println(x);
                LOG.error("", e);
            }
        } finally {
            bytes.writeLimit(limit);
            bytes.writePosition(position);
        }
    }

    /**
     * sets up subscriptions with the server, even if the socket connection is down, the
     * subscriptions will be re-establish with the server automatically once it comes back up. To
     * end the subscription with the server call {@code net.openhft.chronicle.network.connection.TcpChannelHub#unsubscribe(long)}
     *
     * @param asyncSubscription detail of the subscription that you wish to hold with the server
     */
    public void subscribe(@NotNull final AsyncSubscription asyncSubscription) {
        tcpSocketConsumer.subscribe(asyncSubscription);
    }

    /**
     * closes a subscription established by {@code net.openhft.chronicle.network.connection.TcpChannelHub#
     * subscribe(net.openhft.chronicle.network.connection.AsyncSubscription)}
     *
     * @param tid the unique id of this subscription
     */
    public void unsubscribe(final long tid) {
        tcpSocketConsumer.unsubscribe(tid);
    }

    @NotNull
    public ReentrantLock outBytesLock() {
        return outBytesLock;
    }


    private synchronized void doHandShaking(SocketChannel socketChannel) throws IOException {

        final SessionDetails sessionDetails = sessionDetails();
        handShakingWire.clear();
        handShakingWire.bytes().clear();
        handShakingWire.writeDocument(false, wireOut -> {
            if (sessionDetails == null)
                wireOut.writeEventName(userid).text(getProperty("user.name"));
            else
                wireOut.writeEventName(userid).text(sessionDetails.userId());
        });

        writeSocket(handShakingWire, timeoutMs, socketChannel);

    }

    private SessionDetails sessionDetails() {
        if (sessionProvider == null)
            return null;
        return sessionProvider.get();
    }

    /**
     * closes the existing connections and establishes a new closeables
     */
    protected synchronized void closeSocket() {

        if (clientChannel != null) {

            try {
                clientChannel.socket().shutdownInput();
            } catch (IOException ignored) {
            }

            try {
                clientChannel.socket().shutdownOutput();
            } catch (IOException ignored) {
            }

            try {
                clientChannel.socket().close();
            } catch (IOException ignored) {
            }

            try {
                clientChannel.close();
            } catch (IOException ignored) {
            }

            clientChannel = null;

        }


    }

    /**
     * called when we are completed finished with using the TcpChannelHub
     */
    public void close() {

        //  eventLoop.stop();
        tcpSocketConsumer.stop();
        closed = true;

        while (currentState.get() != ConnecitonStatus.DISCONNECTED) {
            Jvm.pause(10);
            System.out.println("waiting for disconnect");
        }


    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    public long nextUniqueTransaction(long time) {
        long id = time;
        for (; ; ) {
            long old = transactionID.get();
            if (old == id)
                id = old + 1;
            if (transactionID.compareAndSet(old, id))
                break;
        }
        return id;
    }

    /**
     * sends data to the server via TCP/IP
     *
     * @param wire the {@code wire} containing the outbound data
     */
    public void writeSocket(@NotNull final WireOut wire) {
        assert outBytesLock().isHeldByCurrentThread();
        checkNotClosed();

        try {
            SocketChannel clientChannel = this.clientChannel;
            if (clientChannel != null)
                // send out all the bytes
                writeSocket(wire, timeoutMs, clientChannel);
            else
                desiredState.set(ConnecitonStatus.RECONNECT);
        } catch (Exception e) {
            desiredState.set(ConnecitonStatus.RECONNECT);
            Jvm.rethrow(e);
        }

    }


    public Wire proxyReply(long timeoutTime, final long tid) {
        checkNotClosed();
        try {
            return tcpSocketConsumer.syncBlockingReadSocket(timeoutTime, tid);
        } catch (IORuntimeException e) {
            throw e;
        } catch (RuntimeException e) {
            closeSocket();
            throw e;
        } catch (Exception e) {
            closeSocket();
            throw Jvm.rethrow(e);
        } catch (AssertionError e) {
            throw e;
        }
    }

    /**
     * writes the bytes to the socket
     *
     * @param outWire     the data that you wish to write
     * @param timeoutTime how long before a we timeout
     * @throws IOException
     */
    private void writeSocket(@NotNull WireOut outWire, long timeoutTime, @NotNull SocketChannel socketChannel) throws
            IOException {


        final Bytes<?> bytes = outWire.bytes();
        long outBytesPosition = bytes.writePosition();

        // if we have other threads waiting to send and the buffer is not full,
        // let the other threads write to the buffer
        if (outBytesLock().hasQueuedThreads() &&
                outBytesPosition + largestChunkSoFar <= tcpBufferSize)
            return;

        final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
        outBuffer.limit((int) bytes.writePosition());

        outBuffer.position(0);

        if (Jvm.IS_DEBUG)
            logToStandardOutMessageSent(outWire, outBuffer);

        upateLargestChunkSoFarSize(outBuffer);

        while (outBuffer.remaining() > 0) {
            checkNotClosed();
            int len = socketChannel.write(outBuffer);

            if (len == -1)
                throw new IORuntimeException("Disconnection to server " + description + "/" + TCPRegistry.lookup(description));

            if (outBuffer.remaining() == 0)
                break;

            if (LOG.isDebugEnabled())
                LOG.debug("Buffer is full");

            // if we have queued threads then we don't have to write all the bytes as the other
            // threads will write the remains bytes.
            if (outBuffer.remaining() > 0 && outBytesLock().hasQueuedThreads() &&
                    outBuffer.remaining() + largestChunkSoFar <= tcpBufferSize) {
                if (LOG.isDebugEnabled())
                    LOG.debug("continuing -  without all the data being written to the buffer as " +
                            "it will be written by the next thread");
                outBuffer.compact();
                bytes.writeLimit(outBuffer.limit());
                bytes.writePosition(outBuffer.position());
                return;
            }

            //checkTimeout(timeoutTime);
        }

        outBuffer.clear();
        bytes.clear();
    }

    private void logToStandardOutMessageSent(@NotNull WireOut wire, @NotNull ByteBuffer outBuffer) {
        if (!YamlLogging.clientWrites)
            return;

        Bytes<?> bytes = wire.bytes();

        final long position = bytes.writePosition();
        final long limit = bytes.writeLimit();
        try {

            bytes.writeLimit(outBuffer.limit());
            bytes.writePosition(outBuffer.position());

            try {
                System.out.println(((!YamlLogging.title.isEmpty()) ? "### " + YamlLogging
                        .title + "\n" : "") + "" +
                        YamlLogging.writeMessage + (YamlLogging.writeMessage.isEmpty() ?
                        "" : "\n\n") +
                        "sends:\n\n" +
                        "```yaml\n" +
                        ((wire instanceof TextWire) ?
                                Wires.fromSizePrefixedBlobs(bytes, bytes.writePosition(), bytes.writeLimit()) :
                                BytesUtil.toHexString(bytes, bytes.writePosition(), bytes.writeRemaining())) +
                        "```");
                YamlLogging.title = "";
                YamlLogging.writeMessage = "";
            } catch (Exception e) {
                LOG.error(Bytes.toString(bytes), e);
            }

        } finally {
            bytes.writeLimit(limit);
            bytes.writePosition(position);
        }
    }

    /**
     * calculates the size of each chunk
     *
     * @param outBuffer the outbound buffer
     */
    private void upateLargestChunkSoFarSize(@NotNull ByteBuffer outBuffer) {
        int sizeOfThisChunk = (int) (outBuffer.limit() - limitOfLast);
        if (largestChunkSoFar < sizeOfThisChunk)
            largestChunkSoFar = sizeOfThisChunk;

        limitOfLast = outBuffer.limit();
    }

    public Wire outWire() {
        assert outBytesLock().isHeldByCurrentThread();
        return outWire;
    }


    private void reflectServerHeartbeatMessage(ValueIn valueIn) {

        // time stamp sent from the server, this is so that the server can calculate the round
        // trip time
        long timestamp = valueIn.int64();

        this.lock(() -> {

            TcpChannelHub.this.writeMetaDataForKnownTID(0, outWire, null, 0);

            TcpChannelHub.this.outWire.writeDocument(false, w ->
                    // send back the time stamp that was sent from the server
                    w.writeEventName(heartbeatReply).int64(timestamp));

            TcpChannelHub.this.writeSocket(outWire);
        });
    }


    public long writeMetaDataStartTime(long startTime, @NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        checkNotClosed();
        startTime(startTime);
        long tid = nextUniqueTransaction(startTime);

        writeMetaDataForKnownTID(tid, wire, csp, cid);

        return tid;
    }

    public void writeMetaDataForKnownTID(long tid, @NotNull Wire wire, @Nullable String csp, long
            cid) {
        assert outBytesLock().isHeldByCurrentThread();
        checkNotClosed();

        wire.writeDocument(true, wireOut -> {
            if (cid == 0)
                wireOut.writeEventName(CoreFields.csp).text(csp);
            else
                wireOut.writeEventName(CoreFields.cid).int64(cid);
            wireOut.writeEventName(CoreFields.tid).int64(tid);
        });
    }

    /**
     * The writes the meta data to wire - the async version does not contain the tid
     *
     * @param wire the wire that we will write to
     * @param csp  provide either the csp or the cid
     * @param cid  provide either the csp or the cid
     */
    public void writeAsyncHeader(@NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        checkNotClosed();
        wire.writeDocument(true, wireOut -> {
            if (cid == 0)
                wireOut.writeEventName(CoreFields.csp).text(csp);
            else
                wireOut.writeEventName(CoreFields.cid).int64(cid);
        });
    }

    public void startTime(long startTime) {
        this.startTime = startTime;
    }

    void checkNotClosed() {
        if (closed)
            throw new IllegalStateException("Closed");
    }

    public void lock(@NotNull Task r) {

        outBytesLock().lock();
        try {
            r.run();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        } finally {
            outBytesLock().unlock();
        }

    }


    public interface Task {
        void run();
    }

    /**
     * uses a single read thread, to process messages to waiting threads based on their {@code tid}
     */
    private class TcpSocketConsumer implements EventHandler {


        private final ExecutorService executorService;

        @NotNull
        private final Map<Long, Object> map = new ConcurrentHashMap<>();
        private volatile boolean isShutdown;
        private Function<Bytes, Wire> wireFunction;
        @Nullable

        private long tid;


        @NotNull
        private ThreadLocal<Wire> syncInWireThreadLocal = withInitial(() -> wire.apply(
                elasticByteBuffer()));


        /**
         * re-establish all the subscriptions to the server, this method calls the {@code
         * net.openhft.chronicle.network.connection.AsyncSubscription#applySubscribe()} for each
         * subscription, this could should establish a subscriotuib with the server.
         */
        private void onReconnect() {
            map.values().forEach(v -> {
                if (v instanceof AsyncSubscription) {
                    if (!(v instanceof AsyncTemporarySubscription))
                        ((AsyncSubscription) v).applySubscribe();
                }
            });
        }

        public void onConnectionClosed() {
            map.values().forEach(v -> {
                if (v instanceof AsyncSubscription) {
                    ((AsyncSubscription) v).onClose();
                } else if (v instanceof Bytes) {
                    synchronized (v) {
                        v.notifyAll();
                    }
                }
            });
        }

        /**
         * @param wireFunction converts bytes into wire, ie TextWire or BinaryWire
         * @param name         the name of the uri of the request that the TcpSocketConsumer is
         *                     running for.
         */
        private TcpSocketConsumer(
                @NotNull final Function<Bytes, Wire> wireFunction,
                @NotNull final String name) {
            this.wireFunction = wireFunction;
            System.out.println("constructor remoteAddress=" + remoteAddress);
            //     attemptConnect();

            executorService = start();
            // used for the heartbeat
            eventLoop.addHandler(this);


        }

        @Override
        public HandlerPriority priority() {
            return HandlerPriority.MONITOR;
        }

        /**
         * blocks this thread until a response is received from the socket
         *
         * @param timeoutTimeMs the amount of time to wait before a time out exceptions
         * @param tid           the {@code tid} of the message that we are waiting for
         * @throws InterruptedException
         */
        private Wire syncBlockingReadSocket(final long timeoutTimeMs, long tid) throws
                InterruptedException, TimeoutException {
            long start = System.currentTimeMillis();

            final Wire wire = syncInWireThreadLocal.get();
            wire.clear();

            Bytes<?> bytes = wire.bytes();
            ((ByteBuffer) bytes.underlyingObject()).clear();

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (bytes) {
                map.put(tid, bytes);
                bytes.wait(timeoutTimeMs);
            }

            logToStandardOutMessageReceived(wire);

            if (System.currentTimeMillis() - start >= timeoutTimeMs) {
                throw new TimeoutException("timeoutTimeMs=" + timeoutTimeMs);
            }

            return wire;

        }

        void subscribe(@NotNull final AsyncSubscription asyncSubscription) {
            map.put(asyncSubscription.tid(), asyncSubscription);
            asyncSubscription.applySubscribe();
        }

        public void unsubscribe(long tid) {
            map.remove(tid);
        }

        /**
         * uses a single read thread, to process messages to waiting threads based on their {@code
         * tid}
         */
        private ExecutorService start() {
            checkNotShutdown();
            awaitingHeartbeat.set(false);
            ExecutorService executorService = newSingleThreadExecutor(
                    new NamedThreadFactory("TcpSocketConsumer-" + name, true));
            isShutdown = false;
            executorService.submit(() -> {
                try {
                    running();
                } catch (IORuntimeException e) {
                    LOG.debug("", e);
                } catch (Throwable e) {
                    if (!isShutdown())
                        LOG.error("", e);
                }
            });

            while (currentState.get() == null) {

            }

            return executorService;

        }

        private void checkNotShutdown() {
            if (isShutdown)
                throw new IllegalStateException("you can not call this method once stop() has " +
                        "been caleld.");
        }


        private void running() {
            attemptConnect();
            final Wire inWire = wireFunction.apply(elasticByteBuffer());
            assert inWire != null;

            while (!isShutdown()) {
                checkConnectionState();
                if (currentState.get() == ConnecitonStatus.DISCONNECTED) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        continue;
                    }
                    continue;
                }
                try {
                    // if we have processed all the bytes that we have read in
                    final Bytes<?> bytes = inWire.bytes();

                    // the number bytes ( still required  ) to read the size
                    blockingRead(inWire, SIZE_OF_SIZE);

                    final int header = bytes.readVolatileInt(0);
                    final long messageSize = size(header);

                    // read the data
                    if (Wires.isData(header)) {
                        assert messageSize < Integer.MAX_VALUE;
                        processData(tid, Wires.isReady(header), header, (int) messageSize, inWire);
                    } else {
                        // read  meta data - get the tid
                        blockingRead(inWire, messageSize);
                        logToStandardOutMessageReceived(inWire);
                        inWire.readDocument((WireIn w) -> this.tid = CoreFields.tid(w), null);
                    }

                } catch (IOException e) {
                    // e.printStackTrace();

                    if (isShutdown()) {
                        break;
                    } else {
                        System.out.println("will reconnect");
                        desiredState.set(ConnecitonStatus.RECONNECT);
                    }


                } finally {
                    clear(inWire);
                }
            }
            System.out.println("disconnecting");

            attemptDisconnect();
        }

        private boolean isShutdown() {
            return isShutdown || currentThread().isInterrupted();
        }

        private void clear(@NotNull final Wire inWire) {
            inWire.clear();
            ((ByteBuffer) inWire.bytes().underlyingObject()).clear();
        }

        /**
         * @param header message size in header form
         * @return the true size of the message
         */
        private long size(int header) {
            final long messageSize = Wires.lengthOf(header);
            assert messageSize > 0 : "Invalid message size " + messageSize;
            assert messageSize < 1 << 30 : "Invalid message size " + messageSize;
            return messageSize;
        }


        /**
         * @param tid         the transaction id of the message
         * @param isReady     if true, this will be the last message for this tid
         * @param header      message size in header form
         * @param messageSize the sizeof the wire message
         * @param inWire      the location the data will be writen to
         * @throws IOException
         */
        private void processData(final long tid, final boolean isReady,
                                 final int header,
                                 final int messageSize, @NotNull Wire inWire) throws IOException {

            long startTime = 0;
            Object o = null;

            for (; !isShutdown(); ) {

                o = isReady ? map.remove(tid) : map.get(tid);
                if (o != null)
                    break;

                // this can occur if the server returns the response before we have started to
                // listen to it

                if (startTime == 0)
                    startTime = System.currentTimeMillis();

                if (System.currentTimeMillis() - startTime > 3_000) {
                    LOG.error("unable to respond to tid=" + tid + ", given that we have received a " +
                            " message we a tid which is unknown, something has become corrupted, " +
                            "so the safest thing to do is to drop the connection to the server and " +
                            "start again.");
                    return;
                }

            }

            // heartbeat message sent from the server
            if (tid == 0) {
                processServerSystemMessage(header, messageSize);
                return;
            }

            // for async
            if (o instanceof AsyncSubscription) {
                System.out.println("processing aysnc");
                blockingRead(inWire, messageSize);
                logToStandardOutMessageReceived(inWire);
                onMessageReceived();
                ((AsyncSubscription) o).onConsumer(inWire);

                // for async
            } else {

                final Bytes bytes = (Bytes) o;
                // for sync
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (bytes) {
                    bytes.clear();
                    final ByteBuffer byteBuffer = (ByteBuffer) bytes.underlyingObject();
                    byteBuffer.clear();
                    // we have to first write the header back to the bytes so that is can be
                    // viewed as a document
                    bytes.writeInt(0, header);
                    byteBuffer.position(SIZE_OF_SIZE);
                    byteBuffer.limit(SIZE_OF_SIZE + messageSize);
                    readBuffer(byteBuffer);
                    onMessageReceived();
                    bytes.readLimit(byteBuffer.position());
                    bytes.notifyAll();
                }
            }

        }

        private Bytes serverHeartBeatHandler = Bytes.elasticByteBuffer();

        /**
         * process system messages which originate from the server
         *
         * @param header
         * @param messageSize
         * @throws IOException
         */
        private void processServerSystemMessage(final int header, final int messageSize)
                throws IOException {
            serverHeartBeatHandler.clear();
            final Bytes bytes = serverHeartBeatHandler;

            bytes.clear();
            final ByteBuffer byteBuffer = (ByteBuffer) bytes.underlyingObject();
            byteBuffer.clear();
            // we have to first write the header back to the bytes so that is can be
            // viewed as a document
            bytes.writeInt(0, header);
            byteBuffer.position(SIZE_OF_SIZE);
            byteBuffer.limit(SIZE_OF_SIZE + messageSize);
            readBuffer(byteBuffer);

            bytes.readLimit(byteBuffer.position());

            final StringBuilder eventName = Wires.acquireStringBuilder();
            wire.apply(bytes).readDocument(null, d -> {
                        final ValueIn valueIn = d.readEventName(eventName);
                        if (heartbeat.contentEquals(eventName))
                            reflectServerHeartbeatMessage(valueIn);

                    }
            );
        }


        /**
         * blocks indefinitely until the number of expected bytes is received
         *
         * @param wire          the wire that the data will be written into, this wire must contain
         *                      an underlying ByteBuffer
         * @param numberOfBytes the size of the data to read
         * @throws IOException if anything bad happens to the socket connection
         */
        private void blockingRead(@NotNull final WireIn wire, final long numberOfBytes)
                throws IOException {

            final Bytes<?> bytes = wire.bytes();
            bytes.ensureCapacity(bytes.readPosition() + numberOfBytes);

            final ByteBuffer buffer = (ByteBuffer) bytes.underlyingObject();
            final int start = (int) bytes.writePosition();
            buffer.position(start);

            buffer.limit((int) (start + numberOfBytes));
            readBuffer(buffer);
            bytes.readLimit(buffer.position());

            onMessageReceived();
        }

        private void readBuffer(@NotNull final ByteBuffer buffer) throws IOException {
            while (buffer.remaining() > 0) {


                if (clientChannel == null || clientChannel.read(buffer) == -1) {
                    // System.out.println("readBuffer will reconncet");
                    throw new IOException("Disconnection to server " + description);
                }
                if (isShutdown)
                    throw new IOException("The server was shutdown, " + description + "/" + TCPRegistry.lookup(description));

            }
        }

        private volatile long lastTimeMessageReceived = System.currentTimeMillis();

        private void onMessageReceived() {
            lastTimeMessageReceived = System.currentTimeMillis();
        }


        /**
         * sends a heartbeat from the client to the server and logs the round trip time
         */
        private void sendHeartbeat() {
            awaitingHeartbeat.set(true);
            long l = System.nanoTime();

            // this denotes that the next message is a system message as it has a null csp

            subscribe(new AbstractAsyncTemporarySubscription(TcpChannelHub.this, null) {
                @Override
                public void onSubscribe(WireOut wireOut) {
                    wireOut.writeEventName(heartbeat).int64(System.currentTimeMillis());
                }

                @Override
                public void onConsumer(WireIn inWire) {
                    awaitingHeartbeat.set(false);
                    long roundTipTimeMicros = NANOSECONDS.toMicros(System.nanoTime() - l);
                    if (LOG.isDebugEnabled())
                        LOG.debug(String.format("{0}:{1}heartbeat round trip time={2}us",
                                description, TCPRegistry.lookup(description),
                                roundTipTimeMicros));
                    inWire.clear();

                }
            });
        }

        /**
         * called when we are completed finished with using the TcpChannelHub, after this method is
         * called you will no loger be able to use this instance to received data
         */
        private void stop() {
            isShutdown = true;

            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                LOG.error("", e);
            }
        }

        // set to true if we have sent a heartbeat and are waiting a response
        private final AtomicBoolean awaitingHeartbeat = new AtomicBoolean();

        /**
         * gets called periodically to monitor the heartbeat
         *
         * @return true, if processing was performed
         * @throws InvalidEventHandlerException
         */
        @Override
        public boolean action() throws InvalidEventHandlerException {

            if (TcpChannelHub.this.closed)
                throw new InvalidEventHandlerException();

            // a heartbeat only gets sent out if we have not received any data in the last
            // HEATBEAT_PING_PERIOD milliseconds
            long millisecondsSinceLastMessageReceived = System.currentTimeMillis() - lastTimeMessageReceived;
            if (millisecondsSinceLastMessageReceived >= HEATBEAT_PING_PERIOD && !awaitingHeartbeat.get())
                sendHeartbeat();


            // if we have not received a message from the server after the HEATBEAT_TIMEOUT_PERIOD
            // we will drop and then re-establish the connection.
            long x = millisecondsSinceLastMessageReceived - HEATBEAT_TIMEOUT_PERIOD;
            if (x > 0) {
                //   System.out.println("millisecondsSinceLastMessageReceived=" + millisecondsSinceLastMessageReceived);
                //System.out.println(" reconnect due to heatbeat failure");
                desiredState.set(ConnecitonStatus.RECONNECT);
            }

            if (TcpChannelHub.this.closed)
                throw new InvalidEventHandlerException();


            return true;
        }

        private void checkConnectionState() {


            if (desiredState.get() == ConnecitonStatus.RECONNECT) {

                ReentrantLock reentrantLock = outBytesLock();
                reentrantLock.lock();
                System.out.println("attempt reconnect remoteAddress=" + remoteAddress);
                try {
                    attemptDisconnect();
                    attemptConnect();
                } finally {
                    reentrantLock.unlock();
                }

            }

            //   if (desiredState.get() == ConnecitonStatus.CONNECTED &&
            //           currentState.get() == ConnecitonStatus.DISCONNECTED) {
            //      attemptConnect();
            //  }

            if (desiredState.get() == ConnecitonStatus.DISCONNECTED &&
                    currentState.get() == ConnecitonStatus.CONNECTED) {
                attemptDisconnect();
            }

        }

        private void attemptDisconnect() {
            ReentrantLock reentrantLock = outBytesLock();
            reentrantLock.lock();
            try {
                closeSocket();
                onDisconnected();
            } catch (Exception e) {

            } finally {
                reentrantLock.unlock();
            }
        }

        private boolean attemptConnect() {
            System.out.println("attemptConnect remoteAddress=" + remoteAddress);
            ReentrantLock reentrantLock = outBytesLock();
            reentrantLock.lock();
            SocketChannel socketChannel;
            try {
                long start = System.currentTimeMillis();
                for (; ; ) {

                    if (start + timeoutMs < System.currentTimeMillis())
                        return false;
                    socketChannel = openSocketChannel();

                    if (socketChannel == null || !socketChannel.connect(remoteAddress)) {
                        Jvm.pause(100);
                        continue;
                    } else
                        break;

                }

                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setReceiveBufferSize(tcpBufferSize);
                socketChannel.socket().setSendBufferSize(tcpBufferSize);

                // the hand-shaking is assigned before setting the clientChannel, so that it can
                // be assured to go first
                doHandShaking(socketChannel);
                clientChannel = socketChannel;
                currentState.set(ConnecitonStatus.CONNECTED);

                // resets the heartbeat timer
                onMessageReceived();

                onConnected();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("failed to connect remoteAddress=" + remoteAddress + " so will reconnect");
                desiredState.set(ConnecitonStatus.RECONNECT);
                currentState.set(ConnecitonStatus.DISCONNECTED);
                if (clientChannel != null)
                    try {
                        clientChannel.close();
                        clientChannel = null;
                    } catch (IOException e1) {

                    }
            } finally {
                reentrantLock.unlock();
            }
            return false;
        }

        private void onDisconnected() {
            currentState.set(ConnecitonStatus.DISCONNECTED);
            desiredState.set(null);
            System.out.println(" disconnected to remoteAddress=" + remoteAddress);
            onConnectionClosed();
        }

        private void onConnected() {
            System.out.println("successfully connected to  remoteAddress=" + remoteAddress);
            onMessageReceived();
            desiredState.set(null);
            currentState.set(ConnecitonStatus.CONNECTED);
            onReconnect();
        }

    }


}
