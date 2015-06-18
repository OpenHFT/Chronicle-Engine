package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.wire.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.network.connection.TcpConnectionHub.*;

/**
 * uses a single read thread, to process messages to waiting threads based on their {@code tid}
 */
public class AsyncTcpConsumer {


    private final ExecutorService executorService;
    private Function<Bytes, Wire> wireFunction;
    private final SocketChannel clientChannel;

    private Map<Long, Consumer<Wire>> map = new ConcurrentHashMap<Long, Consumer<Wire>>();


    public AsyncTcpConsumer(
            @NotNull final Function<Bytes, Wire> wireFunction,
            @NotNull final SocketChannel clientChannel) {
        this.wireFunction = wireFunction;
        this.clientChannel = clientChannel;
        executorService = Executors.newSingleThreadExecutor(r -> new Thread(r, "TcpConnectionHub"));
        start();
    }

    private long tid;

    /**
     * uses a single read thread, to process messages to waiting threads based on their {@code tid}
     */
    private void start() {

        executorService.submit(() -> {
            Wire inWire = wireFunction.apply(Bytes.elasticByteBuffer());

            while (Thread.currentThread().isInterrupted()) {

                try {


                    // if we have processed all the bytes that we have read in
                    final Bytes<?> bytes = inWire.bytes();

                    // the number bytes ( still required  ) to read the size
                    blockingRead(inWire, SIZE_OF_SIZE);

                    final int header = bytes.readVolatileInt();
                    final long messageSize = size(header);

                    // read the document
                    blockingRead(inWire, messageSize);
                    logToStandardOutMessageReceived(inWire);

                    // read the meta processData and get the tid
                    if (Wires.isData(header)) {
                        processData(tid, inWire, Wires.isReady(header));
                        inWire = null;
                    } else {
                        inWire.readDocument((WireIn w) -> this.tid = CoreFields.tid(w), null);

                        // in this case given that we have only read the meta data we can reuse the buffer
                        inWire.clear();
                    }

                } catch (Exception e) {
                    throw Jvm.rethrow(e);
                }
            }
        });


    }

    private long size(int header) {
        final long messageSize = Wires.lengthOf(header);
        assert messageSize > 0 : "Invalid message size " + messageSize;
        assert messageSize < 1 << 30 : "Invalid message size " + messageSize;
        return messageSize;
    }

    private void processData(long tid, @NotNull final Wire wire, boolean isReady) {
        Consumer<Wire> wireConsumer = isReady ? map.remove(tid) : map.get(tid);
        wireConsumer.accept(wire);
    }

    /**
     * blocks indefinatly until the number of expected bytes is received
     *
     * @param wire          the wire that the data will be written into,
     *                      this wire must contain an underlying ByteBuffer
     * @param numberOfBytes the size of the data to read
     * @throws IOException if anything bad happens to the socket connection
     */
    private void blockingRead(Wire wire, long numberOfBytes)
            throws IOException {

        ByteBuffer buffer = (ByteBuffer) wire.bytes().underlyingObject();
        long start = buffer.position();
        buffer.limit((int) (wire.bytes().position() + numberOfBytes));
        buffer.position((int) wire.bytes().position());

        while (buffer.position() - start < numberOfBytes) {
            assert clientChannel != null;

            if (clientChannel.read(buffer) == -1)
                throw new IORuntimeException("Disconnection to server");
        }

    }

}
