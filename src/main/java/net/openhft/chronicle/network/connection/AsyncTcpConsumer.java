package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.wire.CoreFields;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.network.connection.TcpConnectionHub.SIZE_OF_SIZE;
import static net.openhft.chronicle.network.connection.TcpConnectionHub.logToStandardOutMessageReceived;

/**
 * uses a single read thread, to process messages to waiting threads based on their {@code tid}
 */
public class AsyncTcpConsumer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncTcpConsumer.class);

    private final ExecutorService executorService;
    private Function<Bytes, Wire> wireFunction;
    private final SocketConnectionProvider provider;
    @Nullable
    private final ReentrantLock inBytesLock;
    private SocketChannel clientChannel;

    private final Map<Long, Consumer<Wire>> map = new ConcurrentHashMap<>();

    /**
     * @param wireFunction converts bytes into wire, ie TextWire or BinaryWire
     * @param provider     used to re-establish a socket connection when/if the socket connection is
     *                     dropped
     * @param inBytesLock  this can be set to NULL, if the hub is dedicated to a single socket
     *                     connection
     */
    public AsyncTcpConsumer(
            @NotNull final Function<Bytes, Wire> wireFunction,
            @NotNull final SocketConnectionProvider provider,
            @Nullable ReentrantLock inBytesLock) {
        this.wireFunction = wireFunction;
        this.provider = provider;
        this.inBytesLock = inBytesLock;
        this.clientChannel = provider.lazyConnect();
        executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "TcpConnectionHub"));
        start();
    }

    /**
     * once a responce is recived for this {@code tid} the consumer will be called with this
     * response
     *
     * @param tid      the tid that the consumer wishes to receive a Wire for
     * @param consumer the consumer that will be applied to a message assosiated witht the {@code
     *                 tid} provided
     */
    public void apply(long tid, Consumer<Wire> consumer) {
        map.put(tid, consumer);
    }

    private long tid;

    /**
     * uses a single read thread, to process messages to waiting threads based on their {@code tid}
     */
    private void start() {

        executorService.submit(() -> {

            Wire inWire = wireFunction.apply(Bytes.elasticByteBuffer());
            assert inWire != null;

            while (!Thread.currentThread().isInterrupted()) {

                if (inBytesLock != null)
                    inBytesLock.lock();

                try {
                    // if we have processed all the bytes that we have read in
                    final Bytes<?> bytes = inWire.bytes();

                    // the number bytes ( still required  ) to read the size
                    blockingRead(inWire, SIZE_OF_SIZE);

                    final int header = bytes.readVolatileInt(0);
                    final long messageSize = size(header);

                    // read the document
                    blockingRead(inWire, messageSize);
                    logToStandardOutMessageReceived(inWire);

                    // read the meta processData and get the tid
                    if (Wires.isData(header))
                        processData(tid, inWire, Wires.isReady(header));
                    else
                        inWire.readDocument((WireIn w) -> this.tid = CoreFields.tid(w), null);

                    clear(inWire);

                } catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted())
                        this.clientChannel = provider.reConnect();
                } catch (Throwable e) {
                    if (Thread.currentThread().isInterrupted())
                        return;
                    LOG.error("", e);
                } finally {
                    if (inBytesLock != null)
                        inBytesLock.unlock();
                }
            }
        });
    }

    private void clear(final Wire inWire) {
        inWire.clear();
        ((ByteBuffer) inWire.bytes().underlyingObject()).clear();
    }

    private long size(int header) {
        final long messageSize = Wires.lengthOf(header);
        assert messageSize > 0 : "Invalid message size " + messageSize;
        assert messageSize < 1 << 30 : "Invalid message size " + messageSize;
        return messageSize;
    }

    private void processData(final long tid, @NotNull final Wire wire, boolean isReady) {
        final Consumer<Wire> wireConsumer = isReady ? map.remove(tid) : map.get(tid);
        try {
            wireConsumer.accept(wire);
        } catch (Exception e) {
            LOG.error("", e);
        }

    }

    /**
     * blocks indefinitely until the number of expected bytes is received
     *
     * @param wire          the wire that the data will be written into, this wire must contain an
     *                      underlying ByteBuffer
     * @param numberOfBytes the size of the data to read
     * @throws IOException if anything bad happens to the socket connection
     */
    private void blockingRead(@NotNull final Wire wire, final long numberOfBytes)
            throws IOException {

        final ByteBuffer buffer = (ByteBuffer) wire.bytes().underlyingObject();
        final long start = buffer.position();

        buffer.limit((int) (start + numberOfBytes));

        while (buffer.remaining() > 0) {
            assert clientChannel != null;

            if (clientChannel.read(buffer) == -1)
                throw new IORuntimeException("Disconnection to server");
        }

        wire.bytes().limit(buffer.position());

    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("", e);
        }

    }
}
