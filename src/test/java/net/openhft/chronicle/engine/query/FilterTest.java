package net.openhft.chronicle.engine.query;

import junit.framework.TestCase;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Rob Austin.
 */
public class FilterTest extends TestCase {

    @Test
    public void test() throws Exception {

        final Bytes b = Bytes.elasticByteBuffer();
        final TextWire textWire = new TextWire(b);

        Filter expected = new Filter();
        expected.add((SerializablePredicate) o -> true, Operation.OperationType.FILTER);

        textWire.write(() -> "filter").object(expected);

        final Filter actual = textWire.read(() -> "filter").object(Filter.class);

        Assert.assertEquals(1, actual.pipeline.size());
        Assert.assertEquals(Operation.OperationType.FILTER, ((Operation) actual.pipeline.get(0)).op());
    }

}