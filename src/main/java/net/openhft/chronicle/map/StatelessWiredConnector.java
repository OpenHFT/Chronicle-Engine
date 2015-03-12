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

package net.openhft.chronicle.map;

/**
 * Created by Rob Austin
 */

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.network2.WireTcpHandler;
import net.openhft.chronicle.network2.event.EventGroup;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireKey;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.IByteBufferBytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.io.StringWriter;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;

/**
 * @author Rob Austin.
 */
public class StatelessWiredConnector<K extends BytesMarshallable, V extends BytesMarshallable> extends WireTcpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessWiredConnector.class);
    private final NativeBytes inLangBytes = new NativeBytes(0, 0);
    @NotNull
    private final OutMessageAdapter outMessageAdapter = new OutMessageAdapter();
    @NotNull
    private final ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    @NotNull
    private final ByteBuffer inLanByteBuffer = ByteBuffer.allocateDirect(64);
    private final StringBuilder methodName = new StringBuilder();
    private final ChronicleHashInstanceBuilder<ChronicleMap<K, V>> chronicleHashInstanceBuilder;

    Map<Long, Runnable> incompleteWork = new HashMap<>();
    private Wire inWire = new TextWire(Bytes.elasticByteBuffer());
    private Wire outWire = new TextWire(Bytes.elasticByteBuffer());
    final Consumer writeElement = new Consumer<Iterator<byte[]>>() {

        @Override
        public void accept(Iterator<byte[]> iterator) {
            outWire.write(Fields.RESULT);
            outWire.bytes().write(iterator.next());
        }
    };
    final Consumer writeEntry = new Consumer<Iterator<Map.Entry<byte[], byte[]>>>() {

        @Override
        public void accept(Iterator<Map.Entry<byte[], byte[]>> iterator) {

            final Map.Entry<byte[], byte[]> entry = iterator.next();

            outWire.write(Fields.RESULT_KEY);
            outWire.bytes().write(entry.getKey());

            outWire.write(Fields.RESULT_VALUE);
            outWire.bytes().write(entry.getValue());
        }
    };
    private byte localIdentifier;
    private long timestamp;
    private short channelId;
    private List<Replica> channelList;
    private ReplicationHub hub;
    private byte remoteIdentifier;
    private boolean hasEatenFirstByte = false;
    private Runnable out = null;
    private SelectionKey key = null;

    public StatelessWiredConnector(@NotNull final ChronicleHashInstanceBuilder<ChronicleMap<K, V>> chronicleHashInstanceBuilder,
                                   @NotNull final ReplicationHub hub,
                                   byte localIdentifier,
                                   @NotNull final List<Replica> channelList) {
        this(chronicleHashInstanceBuilder, hub);
        this.channelList = channelList;
        this.localIdentifier = localIdentifier;
    }


    public StatelessWiredConnector(ChronicleHashInstanceBuilder<ChronicleMap<K, V>> chronicleHashInstanceBuilder, ReplicationHub hub) {
        this.chronicleHashInstanceBuilder = chronicleHashInstanceBuilder;
        this.hub = hub;
    }


    @Override
    protected void process(Wire in, Wire out) throws StreamCorruptedException {


  //      if(LOG.isDebugEnabled()) {
            System.out.println("--------------------------------------------\nserver read:\n\n" + Bytes.toDebugString(in.bytes()));
    //    }

        this.inWire = in;
        this.outWire = out;
        onEvent();
    }




    private void clearInBuffer() {
        inWire.bytes().clear();
        inWireBuffer().clear();
    }

    private boolean shouldClearInBuffer() {
        return inWire.bytes().position() == inWireBuffer().position();
    }

    /**
     * @return true if remaining space is less than 50%
     */
    private boolean shouldCompactInBuffer() {
        return inWire.bytes().position() > 0 && inWire.bytes().remaining() < (inWireBuffer().capacity() / 2);
    }


    private void compactInBuffer() {
        inWireBuffer().position((int) inWire.bytes().position());
        inWireBuffer().limit(inWireBuffer().position());
        inWireBuffer().compact();

        inWire.bytes().position(0);
        inWire.bytes().limit(0);
    }

    @NotNull
    private ByteBuffer inWireBuffer() {
        return (ByteBuffer) inWire.bytes().underlyingObject();
    }

    private int readSocket(@NotNull SocketChannel socketChannel) throws IOException {
        ByteBuffer dst = inWireBuffer();
        int len = socketChannel.read(dst);
        int readUpTo = dst.position();
        inWire.bytes().limit(readUpTo);
        return len;
    }

    @Nullable
    private Wire nextWireMessage() {
        if (inWire.bytes().remaining() < SIZE_OF_SIZE)
            return null;

        final Bytes<?> bytes = inWire.bytes();

        // the size of the next wire message
        int size = bytes.readUnsignedShort(bytes.position());

        inWire.bytes().ensureCapacity(bytes.position() + size);

        if (bytes.remaining() < size) {
            assert size < 100000;
            return null;
        }

        inWire.bytes().limit(bytes.position() + size);

        // skip the size
        inWire.bytes().skip(SIZE_OF_SIZE);

        return inWire;
    }

    private void writeChunked(long transactionId,
                              @NotNull final Function<Map, Iterator<byte[]>> function,
                              @NotNull final Consumer<Iterator<byte[]>> c) {

        final BytesChronicleMap m = bytesMap(channelId);
        final Iterator<byte[]> iterator = function.apply(m);

        // this allows us to write more data than the buffer will allow
        out = () -> {

            // each chunk has its own transaction-id
            outWire.write(Fields.TRANSACTION_ID).int64(transactionId);

            write(map -> {

                boolean hasNext = iterator.hasNext();
                outWire.write(Fields.HAS_NEXT).bool(hasNext);

                if (hasNext)
                    c.accept(iterator);
                else
                    // setting out to NULL denotes that there are no more chunks
                    out = null;
            });

        };

        out.run();

    }

    @SuppressWarnings("UnusedReturnValue")
    void onEvent() {

        // it is assumed by this point that the buffer has all the bytes in it for this message
        long transactionId = inWire.read(Fields.TRANSACTION_ID).int64();
        timestamp = inWire.read(Fields.TIME_STAMP).int64();
        channelId = inWire.read(Fields.CHANNEL_ID).int16();
        inWire.read(Fields.METHOD_NAME).text(methodName);

        if ("PUT_WITHOUT_ACC".contentEquals(methodName)) {
            writeVoid(bytesMap -> {
                final net.openhft.lang.io.Bytes reader = toReader(inWire, Fields.ARG_1, Fields.ARG_2);
                bytesMap.put(reader, reader, timestamp, identifier());
            });
            return;
        }

        try {

            if ("KEY_SET".contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.keySet().iterator(), writeElement);
                return;
            }

            if ("VALUES".contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.values().iterator(), writeElement);
                return;
            }

            if ("ENTRY_SET".contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.entrySet().iterator(), writeEntry);
                return;
            }

            if ("PUT_ALL".contentEquals(methodName)) {
                putAll(transactionId);
                return;
            }

            // write the transaction id
            outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

            if ("CREATE_CHANNEL".contentEquals(methodName)) {
                writeVoid(() -> {
                    short channelId1 = inWire.read(Fields.ARG_1).int16();
                    chronicleHashInstanceBuilder.replicatedViaChannel(hub.createChannel(channelId1)).create();
                    return null;
                });
                return;
            }


            if ("REMOTE_IDENTIFIER".contentEquals(methodName)) {
                this.remoteIdentifier = inWire.read(Fields.RESULT).int8();
                return;
            }

            if ("LONG_SIZE".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).int64(b.longSize()));
                return;
            }

            if ("IS_EMPTY".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).bool(b.isEmpty()));
                return;
            }

            if ("CONTAINS_KEY".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).
                        bool(b.containsKey(toReader(inWire, Fields.ARG_1))));
                return;
            }

            if ("CONTAINS_VALUE".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).
                        bool(b.containsKey(toReader(inWire, Fields.ARG_1))));
                return;
            }

            if ("GET".contentEquals(methodName)) {

                writeValueUsingDelegate(map -> {
                    byte[] key1 = this.toByteArray(inWire, Fields.ARG_1);
                    return map.get(key1);
                });

                return;
            }

            if ("PUT".contentEquals(methodName)) {
                writeValue(b -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, Fields.ARG_1, Fields.ARG_2);
                    return b.put(reader, reader, timestamp, identifier());
                });

                return;
            }

            if ("REMOVE".contentEquals(methodName)) {
                writeValue(b -> b.remove(toReader(inWire, Fields.ARG_1)));
                return;
            }

            if ("CLEAR".contentEquals(methodName)) {
                writeVoid(BytesChronicleMap::clear);
                return;
            }

            if ("REPLACE".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, Fields.ARG_1, Fields.ARG_2);
                    return bytesMap.replace(reader, reader);
                });
                return;
            }

            if ("REPLACE_WITH_OLD_AND_NEW_VALUE".contentEquals(methodName)) {

                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, Fields.ARG_1, Fields.ARG_2, Fields.ARG_3);
                    return bytesMap.replace(reader, reader);
                });

                return;
            }

            if ("PUT_IF_ABSENT".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, Fields.ARG_1, Fields.ARG_2);
                    return bytesMap.putIfAbsent(reader, reader, timestamp, identifier());
                });
                return;
            }

            if ("REMOVE_WITH_VALUE".contentEquals(methodName)) {
                write(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, Fields.ARG_1, Fields.ARG_2);
                    outWire.write(Fields.RESULT).bool(bytesMap.remove(reader, reader, timestamp, identifier()));
                });
                return;
            }

            if ("TO_STRING".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).text(b.toString()));
                return;
            }

            if ("APPLICATION_VERSION".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).text(applicationVersion()));
                return;
            }

            if ("PERSISTED_DATA_VERSION".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).text(persistedDataVersion()));

                return;
            }

            if ("HASH_CODE".contentEquals(methodName)) {
                write(b -> outWire.write(Fields.RESULT).int32(b.hashCode()));
                return;
            }

            throw new IllegalStateException("unsupported event=" + methodName);

        } finally {

            //  if (len > 4)
            if (EventGroup.IS_DEBUG) {
                long len = outWire.bytes().position() - SIZE_OF_SIZE;
                System.out.println("--------------------------------------------\nserver wrote:\n\n" + Bytes.toDebugString(outWire.bytes(), SIZE_OF_SIZE, len));
            }
        }


    }

    private byte[] toByteArray(net.openhft.lang.io.Bytes bytes) {
        if (bytes == null || bytes.remaining() == 0)
            return new byte[]{};

        if (bytes.remaining() > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        byte[] result = new byte[(int) bytes.remaining()];
        bytes.write(result);
        return result;
    }

    private byte[] toBytes(WireKey fieldName) {

        Wire wire = inWire;
        System.out.println(Bytes.toDebugString(inWire.bytes()));

        ValueIn read = wire.read(fieldName);

        long l = read.readLength();
        if (l > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        int fieldLength = (int) l;

        long endPos = wire.bytes().position() + fieldLength;
        long limit = wire.bytes().limit();

        try {
            byte[] bytes = new byte[fieldLength];

            wire.bytes().read(bytes);
            return bytes;

        } finally

        {
            wire.bytes().position(endPos);
            wire.bytes().limit(limit);
        }
    }

    private void putAll(long transactionId) {

        final BytesChronicleMap bytesMap = bytesMap(StatelessWiredConnector.this.channelId);

        if (bytesMap == null)
            return;

        Runnable runnable = incompleteWork.get(transactionId);

        if (runnable != null) {
            runnable.run();
            return;
        }

        runnable = new Runnable() {

            // we should try and collect the data and then apply it atomically as quickly possible
            final Map<byte[], byte[]> collectData = new HashMap<>();

            @Override
            public void run() {
                if (inWire.read(Fields.HAS_NEXT).bool()) {
                    collectData.put(toBytes(Fields.ARG_1), toBytes(Fields.ARG_2));
                } else {
                    // the old code assumed that all the data would fit into a single buffer
                    // this assumption is invalid
                    if (!collectData.isEmpty()) {
                        bytesMap.delegate.putAll((Map) collectData);
                        incompleteWork.remove(transactionId);
                        outWire.write(Fields.TRANSACTION_ID).int64(transactionId);

                        // todo handle the case where there is an exception
                        outWire.write(Fields.IS_EXCEPTION).bool(false);

                        nofityDataWritten();
                    }
                }
            }
        };

        incompleteWork.put(transactionId, runnable);
        runnable.run();

    }

    private byte identifier() {
        // if the client provides a remote identifier then we will use that otherwise we will use the local identifier
        return remoteIdentifier == 0 ? localIdentifier : remoteIdentifier;
    }

    @NotNull
    private CharSequence persistedDataVersion() {
        BytesChronicleMap bytesChronicleMap = bytesMap(channelId);
        if (bytesChronicleMap == null)
            return "";
        return bytesChronicleMap.persistedDataVersion();
    }

    @NotNull
    private CharSequence applicationVersion() {
        return BuildVersion.version();
    }

    /**
     * creates a lang buffer that holds just the payload of the args
     *
     * @param wire the inbound wire
     * @param args the key names of the {@code wire} args
     * @return a new lang buffer containing the bytes of the args
     */
    private net.openhft.lang.io.Bytes toReader(@NotNull Wire wire, @NotNull WireKey... args) {

        long inSize = wire.bytes().limit();
        final net.openhft.lang.io.Bytes bytes = DirectStore.allocate(inSize).bytes();

        // copy the bytes to the reader
        for (final WireKey field : args) {

            ValueIn read = wire.read(field);
            long fieldLength = read.readLength();

            long endPos = wire.bytes().position() + fieldLength;
            long limit = wire.bytes().limit();

            try {

                final Bytes source = wire.bytes();
                source.limit(endPos);

                // write the size
                bytes.writeStopBit(source.remaining());

                while (source.remaining() > 0) {
                    if (source.remaining() >= 8)
                        bytes.writeLong(source.readLong());
                    else
                        bytes.writeByte(source.readByte());
                }

            } finally {
                wire.bytes().position(endPos);
                wire.bytes().limit(limit);
            }

        }

        return bytes.flip();
    }

    /**
     * creates a lang buffer that holds just the payload of the args
     *
     * @param wire the inbound wire
     * @return a new lang buffer containing the bytes of the args
     */

    // todo remove this method - just added to get it to work for now
    private byte[] toByteArray(@NotNull Wire wire, @NotNull WireKey field) {

        long inSize = wire.bytes().limit();
        // final net.openhft.lang.io.Bytes bytes = DirectStore.allocate(inSize).bytes();


        ValueIn read = wire.read(field);
        long fieldLength = read.readLength();

        long endPos = wire.bytes().position() + fieldLength;
        long limit = wire.bytes().limit();
        byte[] result = new byte[]{};
        try {

            final Bytes source = wire.bytes();
            source.limit(endPos);

            if (source.remaining() > Integer.MAX_VALUE)
                throw new BufferOverflowException();

            // write the size
            result = new byte[(int) source.remaining()];
            source.read(result);


        } finally {
            wire.bytes().position(endPos);
            wire.bytes().limit(limit);
        }


        return result;
    }

    /**
     * gets the map for this channel id
     *
     * @param channelId the ID of the map
     * @return the chronicle map with this {@code channelId}
     */
    @NotNull
    private ReplicatedChronicleMap map(short channelId) {

        // todo this cast is a bit of a hack, improve later
        final ReplicatedChronicleMap replicas =
                (ReplicatedChronicleMap) channelList.get(channelId);

        if (replicas != null)
            return replicas;

        throw new IllegalStateException();
    }

    /**
     * this is used to push the data straight into the entry in memory
     *
     * @param channelId the ID of the map
     * @return a BytesChronicleMap used to update the memory which holds the chronicle map
     */
    @Nullable
    private BytesChronicleMap bytesMap(short channelId) {

        final BytesChronicleMap bytesChronicleMap = (channelId < bytesChronicleMaps.size())
                ? bytesChronicleMaps.get(channelId)
                : null;

        if (bytesChronicleMap != null)
            return bytesChronicleMap;

        // grow the array
        for (int i = bytesChronicleMaps.size(); i <= channelId; i++) {
            bytesChronicleMaps.add(null);
        }

        final ReplicatedChronicleMap delegate = map(channelId);
        final BytesChronicleMap element = new BytesChronicleMap(delegate);
        bytesChronicleMaps.set(channelId, element);
        return element;

    }

    @SuppressWarnings("SameReturnValue")
    private void writeValue(final Function<BytesChronicleMap, net.openhft.lang.io.Bytes> f) {

        write(b -> {

            byte[] fromBytes = toByteArray(f.apply(b));
            boolean isNull = fromBytes.length == 0;
            outWire.write(Fields.RESULT_IS_NULL).bool(isNull);
            if (isNull)
                return;

            outWire.write(Fields.RESULT);
            outWire.bytes().write(fromBytes);

        });


        nofityDataWritten();

    }

    @SuppressWarnings("SameReturnValue")
    private void writeValueUsingDelegate(final Function<ChronicleMap<byte[], byte[]>, byte[]> f) {

        write(b -> {

            byte[] result = f.apply((ChronicleMap) b.delegate);
            boolean isNull = result == null;

            outWire.write(Fields.RESULT_IS_NULL).bool(isNull);
            if (isNull)
                return;

            isNull = result.length == 0;

            outWire.write(Fields.RESULT);
            outWire.bytes().write(result);

        });


        nofityDataWritten();

    }

    private void writeValueTcpRep(@NotNull Consumer<BytesChronicleMap> process) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        inLanByteBuffer.clear();
        inLangBytes.clear();
        outMessageAdapter.markStartOfMessage();

        bytesMap.output = outMessageAdapter.outBuffer;

        try {
            process.accept(bytesMap);
        } catch (Exception e) {

            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(Fields.IS_EXCEPTION).bool(true);
            outWire.write(Fields.EXCEPTION).text(toString(e));
            LOG.error("", e);
            return;
        }

        outMessageAdapter.accept(outWire);
        nofityDataWritten();

    }

    @SuppressWarnings("SameReturnValue")
    private void write(@NotNull Consumer<BytesChronicleMap> c) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        outWire.bytes().mark();
        outWire.write(Fields.IS_EXCEPTION).bool(false);

        try {
            c.accept(bytesMap);
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(Fields.IS_EXCEPTION).bool(true);
            outWire.write(Fields.EXCEPTION).text(toString(e));
            LOG.error("", e);
            return;
        }

        nofityDataWritten();

    }

    @SuppressWarnings("SameReturnValue")
    private void writeVoid(@NotNull Callable r) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        outWire.bytes().mark();
        outWire.write(Fields.IS_EXCEPTION).bool(false);

        try {
            r.call();
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(Fields.IS_EXCEPTION).bool(true);
            outWire.write(Fields.EXCEPTION).text(toString(e));
            LOG.error("", e);
            return;
        }

        nofityDataWritten();

    }

    /**
     * only used for debugging
     */
    @SuppressWarnings("UnusedDeclaration")
    private void showOutWire() {
        System.out.println("pos=" + outWire.bytes().position() + ",bytes=" + Bytes.toDebugString(outWire.bytes(), 0, outWire.bytes().position()));
    }

    @SuppressWarnings("SameReturnValue")
    private void writeVoid(@NotNull Consumer<BytesChronicleMap> process) {

        // skip 4 bytes where we will write the size
        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        try {
            process.accept(bytesMap);
        } catch (Exception e) {
            LOG.error("", e);
            return;
        }


    }

    /**
     * converts the exception into a String, so that it can be sent to c# clients
     */
    private String toString(@NotNull Throwable t) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    public void localIdentifier(byte localIdentifier) {
        this.localIdentifier = localIdentifier;
    }

    private void nofityDataWritten() {
        if (key != null)
            key.interestOps(OP_WRITE | OP_READ);
    }


    static enum Fields implements WireKey {
        HAS_NEXT,
        TIME_STAMP,
        CHANNEL_ID,
        METHOD_NAME,
        TRANSACTION_ID,
        RESULT,
        RESULT_KEY,
        RESULT_VALUE,
        ARG_1,
        ARG_2,
        ARG_3,
        IS_EXCEPTION,
        EXCEPTION,
        RESULT_IS_NULL
    }

    /**
     * used to send data to the wired stateless client, this class converts/adapts a chronicle map
     * lang-bytes output into chronicle-bytes
     */
    private static class OutMessageAdapter {


        @NotNull
        private final IByteBufferBytes outLangBytes = ByteBufferBytes.wrap(ByteBuffer.allocate(1024));
        private Bytes outChronBytes = Bytes.wrap(outLangBytes.buffer());

        // chronicle map will write its result in here
        final MapIOBuffer outBuffer = new MapIOBuffer() {

            @Override
            public void ensureBufferSize(long l) {

                if (outLangBytes.remaining() >= l)
                    return;

                long size = outLangBytes.capacity() + l;
                if (size > Integer.MAX_VALUE)
                    throw new BufferOverflowException();

                // record the current position and limit
                long position = outLangBytes.position();

                // create a new buffer and copy the data into it
                outLangBytes.clear();
                final IByteBufferBytes newOutLangBytes = ByteBufferBytes.wrap(ByteBuffer.allocate((int) size));
                newOutLangBytes.write(outLangBytes);

                outChronBytes = Bytes.wrap(outLangBytes.buffer());

                newOutLangBytes.limit(newOutLangBytes.capacity());
                newOutLangBytes.position(position);

                outChronBytes.limit(outChronBytes.capacity());
                outChronBytes.position(position);
            }

            @NotNull
            @Override
            public net.openhft.lang.io.Bytes in() {
                return outLangBytes;
            }
        };
        private long startChunk;


        void clear() {
            outChronBytes.clear();
            outLangBytes.clear();
        }


        /**
         * adapts the chronicle out lang bytes to chroncile bytes
         *
         * @param wire the wire that we wish to append data to
         */
        void accept(@NotNull Wire wire) {

            wire.write(Fields.IS_EXCEPTION).bool(false);

            // flips calls flip on this message so that we can read it
            flipMessage();

            // set the chron-bytes and the lang-bytes to be the same
            outChronBytes.position(outLangBytes.position());
            outChronBytes.limit(outLangBytes.limit());

            // is Null
            boolean isNull = outChronBytes.readBoolean();

            // read the size - not used
            outChronBytes.readStopBit();

            wire.write(Fields.RESULT_IS_NULL).bool(isNull);
            if (!isNull) {
                // write the result
                wire.write(Fields.RESULT);
                wire.bytes().write(outChronBytes);
            }
        }

        private void flipMessage() {
            long position = outLangBytes.position();
            outLangBytes.position(startChunk);
            outLangBytes.limit(position);
        }

        /**
         * marks the start of the message
         */
        public void markStartOfMessage() {
            startChunk = outLangBytes.position();
        }

    }


}
