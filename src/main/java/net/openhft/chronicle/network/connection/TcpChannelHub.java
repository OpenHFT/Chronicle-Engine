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
import net.openhft.chronicle.core.util.Time;
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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static java.lang.Thread.currentThread;
import static java.lang.ThreadLocal.withInitial;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static net.openhft.chronicle.bytes.Bytes.elasticByteBuffer;
import static net.openhft.chronicle.core.Jvm.*;
import static net.openhft.chronicle.engine.server.internal.SystemHandler.EventId.*;


/**
 * Created by Rob Austin
 */
public class TcpChannelHub implements View, Closeable {

    public static final int HEATBEAT_PING_PERIOD = getInteger("heartbeat.ping.period", 5_000);
    public static final int HEATBEAT_TIMEOUT_PERIOD = getInteger("heartbeat.timeout", 20_000);

    public static final int SIZE_OF_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(TcpChannelHub.class);
    public final long timeoutMs;
    @NotNull
    protected final String name;

    @Override
    public String toString() {
        return "TcpChannelHub{" +
                "remoteAddress=" + remoteAddress +
                ", description='" + description + '}';
    }

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

    private volatile boolean closed;
    private final Wire handShakingWire;
    private final String description;


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
        this.name = remoteAddress.toString();
        this.timeoutMs = 10_000;
        this.wire = wire;
        this.handShakingWire = wire.apply(Bytes.elasticByteBuffer());
        this.sessionProvider = sessionProvider;
        this.tcpSocketConsumer = new TcpSocketConsumer(wire);
    }

    @Nullable
    static SocketChannel openSocketChannel()
            throws IOException {
        SocketChannel result = SocketChannel.open();
        result.socket().setTcpNoDelay(true);
        return result;
    }


    private void onDisconnected() {

        System.out.println(" disconnected to remoteAddress=" + remoteAddress);
        tcpSocketConsumer.onConnectionClosed();
    }

    private void onConnected() {

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
                                        Wires.fromSizePrefixedBlobs(bytes,
                                                bytes.readPosition(), bytes.readRemaining()) :
                                "```\n" +
                                        BytesUtil.toHexString(bytes, bytes.readPosition(),
                                                bytes.readRemaining())

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
        return sessionProvider.get();
    }

    /**
     * closes the existing connections and establishes a new closeables
     */
    protected synchronized void closeSocket() {
        SocketChannel clientChannel = this.clientChannel;
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

            this.clientChannel = null;
            onDisconnected();
        }

    }

    /**
     * called when we are completed finished with using the TcpChannelHub
     */
    public void close() {

        //  eventLoop.stop();
        tcpSocketConsumer.stop();
        closed = true;
        System.out.println("closing " + remoteAddress + "");
        while (clientChannel != null) {
            pause(10);
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

        SocketChannel clientChannel = this.clientChannel;
        if (clientChannel == null)
            throw new IORuntimeException("Not Connected " + remoteAddress);

        try {
            writeSocket(wire, timeoutMs, clientChannel);
        } catch (Exception e) {
            LOG.error("",e);
            closeSocket();
            //reconnect = true;
            throw rethrow(e);
        }

    }


    public Wire proxyReply(long timeoutTime, final long tid) {
        checkNotClosed();
        try {
            return tcpSocketConsumer.syncBlockingReadSocket(timeoutTime, tid);
        } catch (IORuntimeException | AssertionError e) {
            throw e;
        } catch (RuntimeException e) {
            LOG.error("",e);
            closeSocket();
            //reconnect = true;
            throw e;
        } catch (Exception e) {
            LOG.error("",e);
            closeSocket();
            // reconnect = true;
            throw rethrow(e);
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

        final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
        outBuffer.limit((int) bytes.writePosition());

        outBuffer.position(0);

        if (IS_DEBUG)
            logToStandardOutMessageSent(outWire, outBuffer);

        updateLargestChunkSoFarSize(outBuffer);

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

        }

        outBuffer.clear();
        bytes.clear();
    }

    private void logToStandardOutMessageSent(@NotNull WireOut wire, @NotNull ByteBuffer outBuffer) {
        if (!YamlLogging.clientWrites)
            return;

        Bytes<?> bytes = wire.bytes();

        try {

            System.out.println(((!YamlLogging.title.isEmpty()) ? "### " + YamlLogging
                    .title + "\n" : "") + "" +
                    YamlLogging.writeMessage + (YamlLogging.writeMessage.isEmpty() ?
                    "" : "\n\n") +
                    "sends:\n\n" +
                    "```yaml\n" +
                    ((wire instanceof TextWire) ?
                            Wires.fromSizePrefixedBlobs(bytes,
                                    bytes.readPosition(), bytes.readRemaining()) :
                            BytesUtil.toHexString(bytes, bytes.readRemaining(), bytes
                                            .readRemaining()
                            )) +
                    "```");
            YamlLogging.title = "";
            YamlLogging.writeMessage = "";
        } catch (Exception e) {
            LOG.error(Bytes.toString(bytes), e);
        }


    }

    /**
     * calculates the size of each chunk
     *
     * @param outBuffer the outbound buffer
     */
    private void updateLargestChunkSoFarSize(@NotNull ByteBuffer outBuffer) {
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
        long tid = nextUniqueTransaction(startTime);

        writeMetaDataForKnownTID(tid, wire, csp, cid);

        return tid;
    }

    public void writeMetaDataForKnownTID(long tid, @NotNull Wire wire, @Nullable String csp,
                                         long cid) {
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


    void checkNotClosed() {
        if (closed)
            throw new IllegalStateException("Closed");
    }

    public void lock(@NotNull Task r) {
        checkConnection();
        outBytesLock().lock();
        try {
            r.run();
        } catch (Exception e) {
            throw rethrow(e);
        } finally {
            outBytesLock().unlock();
        }

    }

    /**
     * blocks until there is a conneciton
     */
    public void checkConnection() {
        long start = Time.currentTimeMillis();

        while (clientChannel == null) {

            if (tcpSocketConsumer.isShutdown())
                throw new IORuntimeException("Shutdown connection to" + remoteAddress);

            if (start + timeoutMs > Time.currentTimeMillis())
                Jvm.pause(100);
            else
                throw new IORuntimeException("Not connected to " + remoteAddress);
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

        private long tid;

        @NotNull
        private ThreadLocal<Wire> syncInWireThreadLocal = withInitial(() -> wire.apply(
                elasticByteBuffer()));


        /**
         * re-establish all the subscriptions to the server, this method calls the {@code
         * net.openhft.chronicle.network.connection.AsyncSubscription#applySubscribe()} for each
         * subscription, this could should establish a subscriotuib with the server.
         */
        private void reconnect() {

            ReentrantLock lock = outBytesLock();
            lock.lock();
            try {
                map.values().forEach(v -> {
                    if (v instanceof AsyncSubscription) {
                        if (!(v instanceof AsyncTemporarySubscription))
                            ((AsyncSubscription) v).applySubscribe();
                    }
                });


            } finally {
                lock.unlock();
            }
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
         */
        private TcpSocketConsumer(
                @NotNull final Function<Bytes, Wire> wireFunction) {
            this.wireFunction = wireFunction;
            System.out.println("constructor remoteAddress=" + remoteAddress);


            executorService = start();
            // used for the heartbeat


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
            long start = Time.currentTimeMillis();

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

            if (Time.currentTimeMillis() - start >= timeoutTimeMs) {
                throw new TimeoutException("timeoutTimeMs=" + timeoutTimeMs);
            }

            return wire;

        }

        void subscribe(@NotNull final AsyncSubscription asyncSubscription) {

            if (clientChannel == null) {
                map.put(asyncSubscription.tid(), asyncSubscription);
                // not currently connected
                return;
            }

            // we have lock here to prevent a race with the resubscribe upon a reconnection
            final ReentrantLock reentrantLock = outBytesLock();
            reentrantLock.lock();
            try {
                map.put(asyncSubscription.tid(), asyncSubscription);
                asyncSubscription.applySubscribe();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                reentrantLock.unlock();
            }
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

            return executorService;
        }

        private void checkNotShutdown() {
            if (isShutdown)
                throw new IllegalStateException("you can not call this method once stop() has " +
                        "been called.");
        }


        private void running() {

            try {


                final Wire inWire = wireFunction.apply(elasticByteBuffer());
                assert inWire != null;

                while (!isShutdown()) {

                    if (clientChannel == null)
                        checkConnectionState();

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

                        if (isShutdown()) {
                            break;
                        } else {
                            LOG.warn("reconnecting due to unexpected exception", e);
                            e.printStackTrace();
                            closeSocket();

                        }


                    } finally {
                        clear(inWire);
                    }
                }

            } catch (Exception e) {
                if (!isShutdown())
                    e.printStackTrace();
            } finally {
                System.out.println("STOPPING....");
                closeSocket();
            }

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
                    startTime = Time.currentTimeMillis();

                if (Time.currentTimeMillis() - startTime > 3_000) {
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
//                System.out.println("processing async");
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
            bytes.ensureCapacity(bytes.writePosition() + numberOfBytes);

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
                final SocketChannel clientChannel = TcpChannelHub.this.clientChannel;
                if (clientChannel == null || clientChannel.read(buffer) == -1)
                    throw new IOException("Disconnection to server " + description);
                if (isShutdown)
                    throw new IOException("The server was shutdown, " + description + "/" + TCPRegistry.lookup(description));
            }
        }

        private volatile long lastTimeMessageReceived = Time.currentTimeMillis();

        private void onMessageReceived() {
            lastTimeMessageReceived = Time.currentTimeMillis();
        }


        /**
         * sends a heartbeat from the client to the server and logs the round trip time
         */
        private void sendHeartbeat() {

            long l = System.nanoTime();

            // this denotes that the next message is a system message as it has a null csp

            subscribe(new AbstractAsyncTemporarySubscription(TcpChannelHub.this, null) {
                @Override
                public void onSubscribe(WireOut wireOut) {
                    wireOut.writeEventName(heartbeat).int64(Time.currentTimeMillis());
                }

                @Override
                public void onConsumer(@NotNull WireIn inWire) {
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


        long lastheartbeatSentTime = 0;

        /**
         * gets called periodically to monitor the heartbeat
         *
         * @return true, if processing was performed
         * @throws InvalidEventHandlerException
         */
        @Override
        public boolean action() throws InvalidEventHandlerException {

            if (clientChannel == null)
                throw new InvalidEventHandlerException();

            // a heartbeat only gets sent out if we have not received any data in the last
            // HEATBEAT_PING_PERIOD milliseconds
            long currentTime = Time.currentTimeMillis();
            long millisecondsSinceLastMessageReceived = currentTime - lastTimeMessageReceived;
            long millisecondsSinceLastHeatbeatSend = currentTime - lastheartbeatSentTime;

            if (millisecondsSinceLastMessageReceived >= HEATBEAT_PING_PERIOD &&
                    millisecondsSinceLastHeatbeatSend >= HEATBEAT_PING_PERIOD) {
                lastheartbeatSentTime = Time.currentTimeMillis();
                sendHeartbeat();
            }


            // if we have not received a message from the server after the HEATBEAT_TIMEOUT_PERIOD
            // we will drop and then re-establish the connection.
            long x = millisecondsSinceLastMessageReceived - HEATBEAT_TIMEOUT_PERIOD;
            if (x > 0) {
                LOG.warn("reconnecting due to heartbeat failure");
                closeSocket();
                throw new InvalidEventHandlerException();
            }


            return true;
        }

        private void checkConnectionState() {
            if (clientChannel != null)
                return;

            System.out.println("attempt reconnect remoteAddress=" + remoteAddress);
            attemptConnect();
        }


        private void attemptConnect() {
            for (; ; ) {
                System.out.println("attemptConnect remoteAddress=" + remoteAddress);
                SocketChannel socketChannel;
                try {

                    for (; ; ) {

                        socketChannel = openSocketChannel();

                        try {
                            if (socketChannel == null || !socketChannel.connect(remoteAddress)) {
                                pause(100);
                                continue;
                            } else
                                break;
                        } catch (ConnectException e) {
                            pause(100);
                            continue;
                        }

                    }

                    socketChannel.socket().setTcpNoDelay(true);
                    socketChannel.socket().setReceiveBufferSize(tcpBufferSize);
                    socketChannel.socket().setSendBufferSize(tcpBufferSize);

                    // resets the heartbeat timer
                    onMessageReceived();

                    // the hand-shaking is assigned before setting the clientChannel, so that it can
                    // be assured to go first
                    doHandShaking(socketChannel);


                    synchronized (this) {
                        clientChannel = socketChannel;
                    }

                    eventLoop.addHandler(this);
                    System.out.println("successfully connected to remoteAddress=" + remoteAddress);

                    reconnect();
                    onConnected();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("failed to connect remoteAddress=" + remoteAddress + " so will reconnect");
                    closeSocket();
                }

            }
        }


    }


}
