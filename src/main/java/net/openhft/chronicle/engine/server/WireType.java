package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Function;

/**
 * Created by Rob Austin
 */
@Deprecated
public enum WireType implements Function<Bytes, Wire> {
    TEXT {
        @Override
        public Wire apply(Bytes bytes) {
            return new TextWire(bytes);
        }
    }, BINARY {
        @Override
        public Wire apply(Bytes bytes) {
            return new BinaryWire(bytes);
        }
    };

    // todo to be removed
    @Deprecated
    public static Function<Bytes, Wire> wire = BINARY;
}
