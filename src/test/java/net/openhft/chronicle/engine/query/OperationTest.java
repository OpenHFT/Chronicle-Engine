package net.openhft.chronicle.engine.query;

import junit.framework.TestCase;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wires;
import org.junit.Test;

/**
 * @author Rob Austin.
 */
public class OperationTest extends TestCase {

    @Test
    public void test() throws Exception {

        final Bytes b = Bytes.elasticByteBuffer();
        final TextWire textWire = new TextWire(b);

        final Operation operation = new Operation(Operation.OperationType.FILTER, (SerializablePredicate) o -> true);
        textWire.write(() -> "operation").object(operation);


        System.out.println(Wires.fromSizePrefixedBlobs(b));
        final Object object = textWire.read(() -> "operation").object(Object.class);

        System.out.println(object);


    }
}