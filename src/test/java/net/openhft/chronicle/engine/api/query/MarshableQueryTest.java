package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.Wires;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Rob Austin.
 */
public class MarshableQueryTest {

    @Test
    public void testToPredicateFalse() throws Exception {

        VanillaIndexQuery marshableQuery = new VanillaIndexQuery();
        marshableQuery.select(TestBean.class, "value.x == 2");

        boolean test = marshableQuery.filter().test(new TestBean(5));
        Assert.assertEquals(false, test);

    }

    @Test
    public void testToPredicateTrue() throws Exception {

        VanillaIndexQuery marshableQuery = new VanillaIndexQuery();
        marshableQuery.select(TestBean.class, "value.x == 2");

        boolean test = marshableQuery.filter().test(new TestBean(2));
        Assert.assertEquals(true, test);
    }

    @Test
    public void testYaml() throws Exception {
        VanillaIndexQuery marshableQuery = new VanillaIndexQuery();
        marshableQuery.select(TestBean.class, "value.x == 2");

        Bytes b = Bytes.elasticByteBuffer();
        Wire w = new TextWire(b);
        w.write().typedMarshallable(marshableQuery);
        Assert.assertEquals("", Wires.fromSizePrefixedBlobs(w));

    }
}