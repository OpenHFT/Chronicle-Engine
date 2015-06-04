package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Created by peter on 25/05/15.
 */
public class Buffers {
    final Bytes<ByteBuffer> keyBuffer = Bytes.elasticByteBuffer();
    final Bytes<ByteBuffer> valueBuffer = Bytes.elasticByteBuffer();

    static final ThreadLocal<Buffers> BUFFERS = ThreadLocal.withInitial(Buffers::new);

    private Buffers() {
    }
}
