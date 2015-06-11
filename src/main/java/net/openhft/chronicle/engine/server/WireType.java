package net.openhft.chronicle.engine.server;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Function;

/**
 * Created by Rob Austin
 */
@Deprecated
public class WireType {

    // todo to be removed
    @Deprecated
    public static Function<Bytes, Wire> wire = BinaryWire::new;
}
