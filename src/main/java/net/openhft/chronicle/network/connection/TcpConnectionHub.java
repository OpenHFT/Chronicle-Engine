/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.CloseablesManager;
import net.openhft.chronicle.engine.api.SessionDetails;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.api.tree.View;
import net.openhft.chronicle.network.event.EventGroup;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static net.openhft.chronicle.engine.server.WireType.wire;
import static net.openhft.chronicle.wire.CoreFields.reply;

/**
 * Created by Rob Austin
 */
public class TcpConnectionHub implements View, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpConnectionHub.class);
    public static final int SIZE_OF_SIZE = 4;

    @NotNull
    protected final String name;
    @NotNull
    protected final InetSocketAddress remoteAddress;
    public final long timeoutMs;
    protected final int tcpBufferSize;
    private final ReentrantLock inBytesLock = new ReentrantLock(true);
    private final ReentrantLock outBytesLock = new ReentrantLock();

    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);
    private final SessionProvider view;

    @Nullable
    protected CloseablesManager closeables;

    final Wire outWire;
    final Wire inWire;

    private long largestChunkSoFar = 0;

    @Nullable
    private SocketChannel clientChannel;

    // this is a transaction id and size that has been read by another thread.
    private volatile long tid;
    private volatile long lastTransactionId;
    private volatile long parkedTransactionTimeStamp;

    private long limitOfLast = 0;

    // set up in the header
    private long startTime;
    private transient boolean closed;
    private long messageSize;

    public TcpConnectionHub(@NotNull final RequestContext requestContext, final Asset asset) {
        this(requestContext.tcpBufferSize(),
                new InetSocketAddress(requestContext.host(), requestContext.port()),
                requestContext.timeout(),
                asset.findView(SessionProvider.class));
    }

    public TcpConnectionHub(int tcpBufferSize, InetSocketAddress remoteAddress,
                            long timeoutMs, SessionProvider sessionProvider) {
        this.tcpBufferSize = tcpBufferSize;
        this.remoteAddress = remoteAddress;
        this.outWire = wire.apply(Bytes.elasticByteBuffer());
        this.inWire = wire.apply(Bytes.elasticByteBuffer());
        this.name = " connected to " + remoteAddress;
        this.timeoutMs = timeoutMs;

        attemptConnect(remoteAddress);
        view = sessionProvider;
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
            LOG.error("", e);
            if (closeables != null) closeables.closeQuietly();
            clientChannel = null;
        }
    }

    @NotNull
    public ReentrantLock inBytesLock() {
        return inBytesLock;
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

    protected synchronized void lazyConnect(final long timeoutMs,
                                            final InetSocketAddress remoteAddress) {
        if (clientChannel != null)
            return;

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress + " ,name=" + name);

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        while (clientChannel == null) {
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
    }

    private SessionDetails sessionDetails() {
        if (view == null)
            return null;
        return view.get();
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

    /**
     * closes the existing connections and establishes a new closeables
     */
    protected void closeExisting() {
        // ensure that any excising connection are first closed
        if (closeables != null)
            closeables.closeQuietly();
        closeables = null;
    }

    public synchronized void close() {
        closed = true;
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
        assert !inBytesLock().isHeldByCurrentThread();

        final long timeoutTime = startTime + this.timeoutMs;
        try {
            for (; ; ) {
                if (clientChannel == null)
                    lazyConnect(timeoutMs, remoteAddress);
                try {
                    // send out all the bytes
                    writeSocket(wire, timeoutTime);
                    break;
                } catch (java.nio.channels.ClosedChannelException e) {
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
        assert inBytesLock().isHeldByCurrentThread();

        try {
            return proxyReplyThrowable(timeoutTime, tid);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (RuntimeException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        } catch (AssertionError e) {
            LOG.error("name=" + name, e);
            throw e;
        }
    }

    //means that we have a message that we have not yet processed
    private volatile boolean hasMessage = false;

    /**
     * @param timeoutTime if set to zero, will return null if unable to get data
     * @param tid
     * @return returns null, if unsuccessful and timeout is set to zero
     * @throws IOException
     */
    private Wire proxyReplyThrowable(long timeoutTime, long tid) throws IOException {

        for (; ; ) {

            // hasMessage - means that we have a message that we have not yet processed
            if (!hasMessage) {
                inWireCompact((int) inWire.bytes().position());
                long position = inWire.bytes().position();
                long limit = inWire.bytes().limit();

                try {
                    assert inBytesLock().isHeldByCurrentThread();

                    // if we have processed all the bytes that we have read in
                    final Bytes<?> bytes = inWire.bytes();

                    long numberOfBytes = SIZE_OF_SIZE - inWireByteBuffer().position();
                    boolean success = readSocket(numberOfBytes, timeoutTime);
                    if (!success)
                        return null;

                    final int header = bytes.readVolatileInt(0);
                    final long messageSize = Wires.lengthOf(header);

                    assert messageSize > 0 : "Invalid message size " + messageSize;
                    assert messageSize < 1 << 30 : "Invalid message size " + messageSize;

                    // read the document
                    numberOfBytes = SIZE_OF_SIZE + messageSize - inWireByteBuffer().position();
                    boolean successs1 = readSocket(numberOfBytes, timeoutTime);
                    if (!successs1)
                        return null;

                    logToStandardOutMessageReceived(inWire);

                    // read the meta data and get the tid
                    if (!Wires.isData(header)) {
                        inWire.readDocument((WireIn w) -> {
                            this.tid = CoreFields.tid(w);

                            System.out.println("change to this.tid=" + this.tid);

                            if (this.tid != tid)
                                // if the transaction id is not for this thread, park it
                                // and allow another thread to pick it up
                                parkedTransactionTimeStamp = System.currentTimeMillis();

                        }, null);

                        continue;
                    } else {
                        hasMessage = true;
                    }
                } catch (RemoteCallTimeoutException e) {
                    inWire.bytes().position(position);
                    inWire.bytes().limit(limit);
                    throw e;
                }

                for (; ; ) {
                    if (this.tid == tid) {
                        hasMessage = false;
                        return inWire;
                    } else
                        pause();

                    if (timeoutTime == 0)
                        return null;

                    if (System.currentTimeMillis() - timeoutTime > parkedTransactionTimeStamp) {
                        hasMessage = false;
                        throw new IllegalStateException("Skipped Message with " +
                                "transaction-id=" +
                                tid +
                                ", this can occur when you have another thread which has called the " +
                                "stateless client and terminated abruptly before the message has been " +
                                "returned from the server and hence consumed by the other thread.");

                    }
                }
            }
        }

    }


    /**
     * clears the wire and its underlying byte buffer
     */
    private void inWireCompact(int pos) {
        if (pos == 0)
            return;
        final ByteBuffer byteBuffer = inWireByteBuffer();
        byteBuffer.position(pos);
        byteBuffer.compact();
        inWire.bytes().position(0);
        byteBuffer.position(0);

    }

    private void pause() {
        assert !outBytesLock().isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        /// don't call inBytesLock.isHeldByCurrentThread() as it not atomic
        inBytesLock().unlock();
        Jvm.pause(1);
        // allows another thread to enter here
        inBytesLock().lock();
    }

    /**
     * reads up to the number of byte in {@code requiredNumberOfBytes} from the socket
     *
     * @param requiredNumberOfBytes the number of bytes to read, if the number of bytes is negative
     *                              then this method imediatly returns without doing anything
     * @param timeoutTime           timeout in milliseconds
     * @throws java.io.IOException socket failed to read data
     */

    private boolean readSocket(long requiredNumberOfBytes, long timeoutTime) throws IOException {
        assert requiredNumberOfBytes < Integer.MAX_VALUE;
        assert inBytesLock().isHeldByCurrentThread();

        if (requiredNumberOfBytes <= 0)
            return false;

        int numberOfBytes = (int) requiredNumberOfBytes;
        ByteBuffer buffer = inWireByteBuffer();
        int position = buffer.position();

        try {
            buffer.limit(position + numberOfBytes);
        } catch (IllegalArgumentException e) {
            buffer = inWireByteBuffer(position + numberOfBytes);
            buffer.limit(position + numberOfBytes);
            buffer.position(position);
        }

        long start = buffer.position();

        while (buffer.position() - start < numberOfBytes) {
            assert clientChannel != null;

            if (clientChannel.read(buffer) == -1)
                throw new IORuntimeException("Disconnection to server");

            boolean success = checkTimeout(timeoutTime);

            if (!success)
                return false;
        }

        final Bytes<?> bytes = inWire.bytes();
        bytes.limit(inWireByteBuffer().position());
        return true;
    }

    @NotNull
    private ByteBuffer inWireByteBuffer() {
        return (ByteBuffer) inWire.bytes().underlyingObject();
    }

    @NotNull
    private ByteBuffer inWireByteBuffer(long requiredCapacity) {
        final Bytes<?> bytes = inWire.bytes();
        bytes.ensureCapacity(requiredCapacity);
        return (ByteBuffer) bytes.underlyingObject();
    }

    /**
     * writes the bytes to the socket
     *
     * @param outWire     the data that you wish to write
     * @param timeoutTime how long before a we timeout
     * @throws java.io.IOException
     */
    private void writeSocket(@NotNull Wire outWire, long timeoutTime) throws IOException {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        final Bytes<?> bytes = outWire.bytes();
        long outBytesPosition = bytes.position();

        // if we have other threads waiting to send and the buffer is not full,
        // let the other threads write to the buffer
        if (outBytesLock().hasQueuedThreads() &&
                outBytesPosition + largestChunkSoFar <= tcpBufferSize)
            return;

        final ByteBuffer outBuffer = (ByteBuffer) bytes.underlyingObject();
        outBuffer.limit((int) bytes.position());

        outBuffer.position(0);

        if (EventGroup.IS_DEBUG)
            logToStandardOutMessageSent(outWire, outBuffer);

        upateLargestChunkSoFarSize(outBuffer);

        while (outBuffer.remaining() > 0) {
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
                bytes.limit(outBuffer.limit());
                bytes.position(outBuffer.position());
                return;
            }

            checkTimeout(timeoutTime);
        }

        outBuffer.clear();
        bytes.clear();
    }

    private void logToStandardOutMessageSent(@NotNull Wire wire, @NotNull ByteBuffer outBuffer) {
        if (!YamlLogging.clientWrites || !Jvm.isDebug())
            return;

        Bytes<?> bytes = wire.bytes();

        final long position = bytes.position();
        final long limit = bytes.limit();
        try {

            bytes.limit(outBuffer.limit());
            bytes.position(outBuffer.position());

            if (YamlLogging.clientWrites) {
                try {

                    System.out.println(((!YamlLogging.title.isEmpty()) ? "### " + YamlLogging
                            .title + "\n" : "") + "" +
                            YamlLogging.writeMessage + (YamlLogging.writeMessage.isEmpty() ?
                            "" : "\n\n") +
                            "sends:\n\n" +
                            "```yaml\n" +
                            ((wire instanceof TextWire) ?
                                    Wires.fromSizePrefixedBlobs(bytes) :
                                    BytesUtil.toHexString(bytes, bytes.position(), bytes.remaining())) +

                            "```");
                    YamlLogging.title = "";
                    YamlLogging.writeMessage = "";
                } catch (Exception e) {
                    System.out.println(
                            Bytes.toDebugString(bytes));
                }
            }

        } finally {
            bytes.limit(limit);
            bytes.position(position);
        }
    }

    private void logToStandardOutMessageReceived(@NotNull Wire wire) {
        Bytes<?> bytes = wire.bytes();

        if (!YamlLogging.clientReads || !Jvm.isDebug())
            return;

        final long position = bytes.position();
        final long limit = bytes.limit();
        try {
            try {
                System.out.println("\nreceives:\n\n" +
                        "```yaml\n" +

                        ((wire instanceof TextWire) ?
                                Wires.fromSizePrefixedBlobs(bytes) :
                                BytesUtil.toHexString(bytes, bytes.position(), bytes.remaining())

                        ) +
                        "```\n\n");
                net.openhft.chronicle.wire.YamlLogging.title = "";
                net.openhft.chronicle.wire.YamlLogging.writeMessage = "";
            } catch (Exception e) {
                System.out.println(Bytes.toDebugString(bytes));
            }
        } finally {
            bytes.limit(limit);
            bytes.position(position);
        }
    }

    /**
     * calculates the size of each chunk
     *
     * @param outBuffer
     */
    private void upateLargestChunkSoFarSize(@NotNull ByteBuffer outBuffer) {
        int sizeOfThisChunk = (int) (outBuffer.limit() - limitOfLast);
        if (largestChunkSoFar < sizeOfThisChunk)
            largestChunkSoFar = sizeOfThisChunk;

        limitOfLast = outBuffer.limit();
    }

    /**
     * @param eventName the event name
     * @param startTime the time the message was sent
     * @param wire
     * @param csp       the csp describing this nammed channel
     * @param cid       if the cid != 0 the cid will be used instead of the csp
     * @return the tid
     */
    private long proxySend(@NotNull final WireKey eventName,
                           final long startTime,
                           @NotNull final Wire wire,
                           @NotNull final String csp,
                           long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        // send
        outBytesLock().lock();
        try {
            long tid = writeMetaDataStartTime(startTime, wire, csp, cid);
            wire.writeDocument(false, wireOut -> {
                wireOut.writeEventName(eventName);
                wireOut.writeValue().marshallable(w -> {
                });
            });

            writeSocket(wire);
            return tid;
        } finally {
            outBytesLock().unlock();
        }
    }

 /*   @SuppressWarnings("SameParameterValue")
    @Nullable
    public String proxyReturnString(@NotNull final WireKey messageId, String csp, long cid) {
        return proxyReturnString(messageId, outWire, csp, cid);
    }*/

    @SuppressWarnings("SameParameterValue")
    @Nullable
    String proxyReturnString(@NotNull final WireKey eventId, @NotNull Wire outWire,
                             @NotNull String csp, long cid) {
        final long startTime = System.currentTimeMillis();
        long tid;

        outBytesLock().lock();
        try {
            tid = proxySend(eventId, startTime, outWire, csp, cid);
        } finally {
            outBytesLock().unlock();
        }

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock().lock();
        try {
            final Wire wire = proxyReply(timeoutTime, tid);

            int datalen = wire.bytes().readVolatileInt();

            assert Wires.isData(datalen);

            return wire.read(reply).text();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            inBytesLock().unlock();
        }
    }

    public Wire outWire() {
        assert outBytesLock().isHeldByCurrentThread();
        return outWire;
    }

    public long writeMetaDataStartTime(long startTime, @NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();
        startTime(startTime);
        long tid = nextUniqueTransaction(startTime);

        writeMetaDataForKnownTID(tid, wire, csp, cid);

        return tid;
    }

    public void writeMetaDataForKnownTID(long tid, @NotNull Wire wire, String csp, long cid) {
        assert outBytesLock().isHeldByCurrentThread();

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

    public static TcpConnectionHub hub(final RequestContext context, @NotNull final Asset asset)
            throws AssetNotFoundException {
        return asset.root().acquireView(TcpConnectionHub.class, context);
    }

    public boolean isClosed() {
        return closed;
    }

}
