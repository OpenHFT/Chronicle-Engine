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
import net.openhft.chronicle.core.io.CloseablesManager;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.network.api.session.SessionDetails;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.WireType.wire;

/**
 * Created by Rob Austin
 */
public class TcpChannelHub implements View, Closeable, SocketChannelProvider {

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
    @Nullable
    protected CloseablesManager closeables;
    private long largestChunkSoFar = 0;

    @Nullable
    private SocketChannel clientChannel;

    private long limitOfLast = 0;

    // set up in the header
    private long startTime;
    private volatile boolean closed;

    public TcpChannelHub(SessionProvider sessionProvider, @NotNull String hostname, int port) {
        this.tcpBufferSize = 64 << 10;
        this.remoteAddress = new InetSocketAddress(hostname, port);
        this.outWire = wire.apply(Bytes.elasticByteBuffer());
        this.inWire = wire.apply(Bytes.elasticByteBuffer());
        this.name = " connected to " + remoteAddress.toString();
        this.timeoutMs = 10_000;

        attemptConnect(remoteAddress);
        tcpSocketConsumer = new TcpSocketConsumer(wire, this, this.remoteAddress.toString());
        this.sessionProvider = sessionProvider;
    }

    @Nullable
    static SocketChannel openSocketChannel(@NotNull final CloseablesManager closeables)
            throws IOException {
        SocketChannel result = null;
        try {
            result = SocketChannel.open();
            result.socket().setTcpNoDelay(true);
        } finally {
            if (result != null)
                try {
                    closeables.add(result);
                } catch (IllegalStateException e) {
                    // already closed
                }
        }
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
     * subsubscription will be re-establish with the server automatically once it comes back up.
     * To end the subscription with the server call
     * {@code net.openhft.chronicle.network.connection.TcpChannelHub#unsubscribe(long)}
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

    private synchronized void attemptConnect(final InetSocketAddress remoteAddress) {

        // ensures that the excising connection are closed
        closeExisting();

        if (closeables == null)
            closeables = new CloseablesManager();

        try {
            SocketChannel socketChannel = openSocketChannel(closeables);
            if (socketChannel != null && socketChannel.connect(remoteAddress)) {
                clientChannel = socketChannel;
                clientChannel.configureBlocking(false);
                clientChannel.socket().setTcpNoDelay(true);
                clientChannel.socket().setReceiveBufferSize(tcpBufferSize);
                clientChannel.socket().setSendBufferSize(tcpBufferSize);
                doHandShaking();
            }
        } catch (IOException e) {
            LOG.error("Failed to connect to " + remoteAddress, e);
            if (closeables != null) closeables.closeQuietly();
            clientChannel = null;
        }
    }

    @NotNull
    public ReentrantLock outBytesLock() {
        return outBytesLock;
    }

    /**
     * @param timeoutTime throws a RemoteCallTimeoutException if the timeout has passed, ignored if
     *                    timeout is zero
     */
    private boolean checkTimeout(long timeoutTime) {
        if (timeoutTime == 0)
            return false;

        if (timeoutTime < System.currentTimeMillis() && !Jvm.isDebug())
            throw new RemoteCallTimeoutException("timeout=" + timeoutTime + "ms");
        return true;
    }

    @Nullable
    @Override
    public SocketChannel lazyConnect() {
        lazyConnect(timeoutMs, remoteAddress);
        return clientChannel;
    }

    @Nullable
    @Override
    public synchronized SocketChannel reConnect() {
        close();
        lazyConnect(timeoutMs, remoteAddress);
        return clientChannel;
    }

    @Nullable
    public synchronized SocketChannel lazyConnect(final long timeoutMs,
                                                  final InetSocketAddress remoteAddress) {
        if (clientChannel != null)
            return clientChannel;

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress + " ,name=" + name);

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        while (clientChannel == null) {
            checkClosed();
            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                if (closeables == null)
                    closeables = new CloseablesManager();

                clientChannel = openSocketChannel(closeables);
                if (clientChannel == null || !clientChannel.connect(remoteAddress)) {
                    Jvm.pause(100);
                    continue;
                }

                clientChannel.socket().setTcpNoDelay(true);
                clientChannel.socket().setReceiveBufferSize(tcpBufferSize);
                clientChannel.socket().setSendBufferSize(tcpBufferSize);
                doHandShaking();

            } catch (IOException e) {
                if (closeables != null) closeables.closeQuietly();
                clientChannel = null;
            } catch (Exception e) {
                if (closeables != null) closeables.closeQuietly();
                throw e;
            }
        }
        return clientChannel;
    }

    private void doHandShaking() {
        outBytesLock().lock();
        try {

            final SessionDetails sessionDetails = sessionDetails();

            outWire().writeDocument(false, wireOut -> {
                if (sessionDetails == null)
                    wireOut.write(() -> "userid").text(System.getProperty("user.name"));
                else
                    wireOut.write(() -> "userid").text(sessionDetails.userId());
            });

            writeSocket(outWire());

        } finally {
            outBytesLock().unlock();
        }

        // re-established all the subscription
        if (tcpSocketConsumer != null)
            tcpSocketConsumer.onReconnect();
    }

    private SessionDetails sessionDetails() {
        if (sessionProvider == null)
            return null;
        return sessionProvider.get();
    }

    /**
     * closes the existing connections and establishes a new closeables
     */
    protected void closeExisting() {
        // ensure that any excising connection are first closed
        if (closeables != null)
            closeables.closeQuietly();
        closeables = null;
        if (tcpSocketConsumer != null)
            tcpSocketConsumer.onConnectionClosed();
    }

    public synchronized void close() {
        closed = true;
        tcpSocketConsumer.close();

        if (closeables != null)
            closeables.closeQuietly();
        closeables = null;
        clientChannel = null;
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
            if (old >= id) id = old + 1;
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
    public void writeSocket(@NotNull final Wire wire) {
        assert outBytesLock().isHeldByCurrentThread();
        checkClosed();

        final long timeoutTime = startTime + this.timeoutMs;
        try {
            for (; ; ) {
                if (clientChannel == null)
                    lazyConnect(timeoutMs, remoteAddress);
                try {
                    // send out all the bytes
                    writeSocket(wire, timeoutTime);
                    break;
                } catch (ClosedChannelException e) {
                    checkTimeout(timeoutTime);
                    lazyConnect(timeoutMs, remoteAddress);
                }
            }
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public Wire proxyReply(long timeoutTime, final long tid) {
        checkClosed();
        try {
            return tcpSocketConsumer.syncBlockingReadSocket(timeoutTime, tid);
        } catch (RuntimeException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw Jvm.rethrow(e);
        } catch (AssertionError e) {
            LOG.error("name=" + name, e);
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
    private void writeSocket(@NotNull Wire outWire, long timeoutTime) throws IOException {

        assert outBytesLock().isHeldByCurrentThread();

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
            checkClosed();
            int len = clientChannel.write(outBuffer);

            if (len == -1)
                throw new IORuntimeException("Disconnection to server");

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

            checkTimeout(timeoutTime);
        }

        outBuffer.clear();
        bytes.clear();
    }

    private void logToStandardOutMessageSent(@NotNull Wire wire, @NotNull ByteBuffer outBuffer) {
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

    public long writeMetaDataStartTime(long startTime, @NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        checkClosed();
        startTime(startTime);
        long tid = nextUniqueTransaction(startTime);

        writeMetaDataForKnownTID(tid, wire, csp, cid);

        return tid;
    }

    public void writeMetaDataForKnownTID(long tid, @NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        checkClosed();

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
        checkClosed();
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

    void checkClosed() {
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
        void run() throws Exception;
    }

    /**
     * uses a single read thread, to process messages to waiting threads based on their {@code tid}
     */
    private static class TcpSocketConsumer implements Closeable {

        @NotNull
        private final ExecutorService executorService;
        @NotNull
        private final SocketChannelProvider provider;
        private final Map<Long, Object> map = new ConcurrentHashMap<>();

        private volatile boolean closeSocketConsumer;
        private Function<Bytes, Wire> wireFunction;
        @Nullable
        private SocketChannel clientChannel;
        private long tid;
        @NotNull
        private ThreadLocal<Wire> syncInWireThreadLocal = ThreadLocal.withInitial(() -> wire.apply(Bytes
                .elasticByteBuffer()));

        /**
         * re-establish all the subscriptions to the server, this method calls the
         * {@code net.openhft.chronicle.network.connection.AsyncSubscription#applySubscribe()}
         * for each subscription, this could should establish a subscriotuib with the server.
         */
        private void onReconnect() {

            map.values().forEach(v -> {
                if (v instanceof AsyncSubscription) {
                    ((AsyncSubscription) v).applySubscribe();
                }
            });
        }

        public void onConnectionClosed() {
            map.values().forEach(v -> {
                if (v instanceof AsyncSubscription) {
                    ((AsyncSubscription) v).onClose();
                }
            });
        }

        /**
         * @param wireFunction converts bytes into wire, ie TextWire or BinaryWire
         * @param provider     used to re-establish a socket connection when/if the socket
         * @param name         the name of the uri of the request that the TcpSocketConsumer is
         *                     running for.
         */
        private TcpSocketConsumer(
                @NotNull final Function<Bytes, Wire> wireFunction,
                @NotNull final SocketChannelProvider provider,
                @NotNull final String name) {
            this.wireFunction = wireFunction;
            this.provider = provider;
            this.clientChannel = provider.lazyConnect();
            executorService = Executors.newSingleThreadExecutor(
                    new NamedThreadFactory("TcpSocketConsumer-" + name, true));

            start();
        }

        /**
         * blocks this thread until a response is received from the socket
         *
         * @param timeoutTimeMs the amount of time to wait before a time out exceptions
         * @param tid           the {@code tid} of the message that we are waiting for
         * @throws InterruptedException
         */
        Wire syncBlockingReadSocket(final long timeoutTimeMs, long tid) throws
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

        private void subscribe(@NotNull final AsyncSubscription asyncSubscription) {
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
        private void start() {

            executorService.submit(() -> {
                try {
                    running();
                } catch (Throwable e) {
                    if (!isClosed())
                        LOG.error("", e);
                }
            });

        }

        private void running() {
            Wire inWire = wireFunction.apply(Bytes.elasticByteBuffer());
            assert inWire != null;

            while (!isClosed()) {
                try {
                    // if we have processed all the bytes that we have read in
                    final Bytes<?> bytes = inWire.bytes();

                    // the number bytes ( still required  ) to read the size
                    blockingRead(inWire, SIZE_OF_SIZE);

                    final int header = bytes.readVolatileInt(0);
                    final long messageSize = size(header);

                    // read the meta processData and get the tid
                    if (Wires.isData(header)) {
                        assert messageSize < Integer.MAX_VALUE;
                        processData(tid, Wires.isReady(header), header, (int) messageSize, inWire);
                    } else {
                        // read the document
                        blockingRead(inWire, messageSize);
                        logToStandardOutMessageReceived(inWire);
                        inWire.readDocument((WireIn w) -> this.tid = CoreFields.tid(w), null);
                    }

                } catch (IOException e) {
                    if (!isClosed())
                        this.clientChannel = provider.reConnect();
                } finally {
                    clear(inWire);
                }
            }
        }

        private boolean isClosed() {
            return closeSocketConsumer || Thread.currentThread().isInterrupted();
        }

        private void clear(@NotNull final Wire inWire) {
            inWire.clear();
            ((ByteBuffer) inWire.bytes().underlyingObject()).clear();
        }

        private long size(int header) {
            final long messageSize = Wires.lengthOf(header);
            assert messageSize > 0 : "Invalid message size " + messageSize;
            assert messageSize < 1 << 30 : "Invalid message size " + messageSize;
            return messageSize;
        }

        private void processData(final long tid, final boolean isReady, final int
                header, final int messageSize, @NotNull Wire inWire) throws IOException {

            long startTime = 0;
            Object o;

            for (; ; ) {
                o = isReady ? map.remove(tid) : map.get(tid);
                if (o != null)
                    break;

                // this can occur if the server returns the response before we have started to
                // listen to it

                if (startTime == 0)
                    startTime = System.currentTimeMillis();

                if (System.currentTimeMillis() - startTime > 1000) {
                    LOG.error("unable to respond to tid=" + tid);
                    blockingRead(inWire, messageSize);
                    return;
                }

            }

            // for async
            if (o instanceof AsyncSubscription) {
                blockingRead(inWire, messageSize);
                logToStandardOutMessageReceived(inWire);
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

                    bytes.readLimit(byteBuffer.position());
                    bytes.notifyAll();
                }
            }

        }

        /**
         * blocks indefinitely until the number of expected bytes is received
         *
         * @param wire          the wire that the data will be written into, this wire must contain
         *                      an underlying ByteBuffer
         * @param numberOfBytes the size of the data to read
         * @throws IOException if anything bad happens to the socket connection
         */
        private void blockingRead(@NotNull final Wire wire, final long numberOfBytes)
                throws IOException {

            Bytes<?> bytes = wire.bytes();
            bytes.ensureCapacity(bytes.readPosition() + numberOfBytes);

            final ByteBuffer buffer = (ByteBuffer) bytes.underlyingObject();
            final int start = (int) bytes.writePosition();
            buffer.position(start);

            buffer.limit((int) (start + numberOfBytes));
            readBuffer(buffer);
            bytes.readLimit(buffer.position());

        }

        private void readBuffer(@NotNull final ByteBuffer buffer) throws IOException {
            while (buffer.remaining() > 0) {
                if (clientChannel == null || clientChannel.read(buffer) == -1)
                    throw new IORuntimeException("Disconnection to server");
                if (closeSocketConsumer)
                    throw new ClosedChannelException();
            }
        }

        @Override
        public void close() {
            closeSocketConsumer = true;
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.error("", e);
            }
            try {
                if (clientChannel != null)
                    clientChannel.close();
            } catch (IOException e) {
                LOG.error("", e);
            }

        }
    }

}
