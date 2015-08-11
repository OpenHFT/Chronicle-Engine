package net.openhft.chronicle.engine.query;

/**
 * @author Rob Austin.
 */

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

import java.io.Serializable;

public class Operation implements Marshallable {

    private OperationType op;
    private Serializable serializable;

    public Operation() {

    }

    public OperationType op() {
        return op;
    }

    public <T extends Serializable> T serializable() {
        return (T) serializable;
    }

    public Operation(@NotNull final OperationType filter,
                     @NotNull final Serializable serializable) {
        this.op = filter;
        this.serializable = serializable;
    }

    @Override
    public void readMarshallable(WireIn wireIn) throws IllegalStateException {
        this.op = wireIn.read(() -> "op").object(OperationType.class);
        this.serializable = wireIn.read(() -> "serializable").object(Serializable.class);
    }

    @Override
    public void writeMarshallable(WireOut wireOut) {
        wireOut.write(() -> "op").object(op);
        wireOut.write(() -> "serializable").object(serializable);
    }

    public enum OperationType {
        MAP, FILTER, PROJECT, FLAT_MAP
    }

}