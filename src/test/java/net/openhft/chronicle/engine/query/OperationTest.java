package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Rob Austin.
 */

@RunWith(value = Parameterized.class)
public class OperationTest extends ThreadMonitoringTest {

    private final WireType wireType;

    @Rule
    public TestName name = new TestName();

    public OperationTest(WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws IOException {

        final List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{WireType.BINARY});
        list.add(new Object[]{WireType.TEXT});
        return list;
    }

    @Test
    public void test() throws Exception {

        final Bytes b = Bytes.elasticByteBuffer();
        final Wire wire = wireType.apply(b);

        final Operation operation = new Operation(Operation.OperationType.FILTER, (SerializablePredicate) o -> true);
        wire.write(() -> "operation").object(operation);

        System.out.println(Wires.fromSizePrefixedBlobs(b));
        final Object object = wire.read(() -> "operation").object(Object.class);

        Assert.assertNotNull(object);
    }
}