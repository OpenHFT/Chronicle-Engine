package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.Wire;

import java.util.function.Function;

/**
 * Created by Rob Austin
 */
public class WireType {

    // todo to be removed
    public static Function<Bytes, Wire> wire = BinaryWire::new;
}
