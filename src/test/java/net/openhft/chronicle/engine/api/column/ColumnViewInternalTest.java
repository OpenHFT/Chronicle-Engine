package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.map.MapWrappingColumnView;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.function.Predicate;

/**
 * Created by rob on 19/12/2016.
 */
public class ColumnViewInternalTest {

    @Test
    public void test() {

        MapWrappingColumnView cv = ObjectUtils.newInstance(MapWrappingColumnView.class);

        ArrayList results = new ArrayList();
        int[] numbers = {1, 2, 3, 4};

        Predicate<Number> predicate = cv.toPredicate("(2,4]");
        for (Number n : numbers) {

            if (predicate.test(n))
                results.add(n);
        }

        Assert.assertEquals("[3, 4]", results.toString());
    }

    @Test
    public void test2() {

        MapWrappingColumnView cv = ObjectUtils.newInstance(MapWrappingColumnView.class);

        ArrayList results = new ArrayList();
        int[] numbers = {1, 2, 3, 4,5};
        Predicate<Number> predicate = cv.toPredicate("(2,4)");
        for (Number n : numbers) {

            if (predicate.test(n))
                results.add(n);
        }

        Assert.assertEquals("[3]", results.toString());
    }

    @Test
    public void testRange() {
        Assert.assertTrue(ColumnViewInternal.DOp.toPredicate("4]", false).test(3));
        Assert.assertTrue(ColumnViewInternal.DOp.toPredicate("3]", false).test(3));
        Assert.assertFalse(ColumnViewInternal.DOp.toPredicate("3)", false).test(3));
        Assert.assertTrue(ColumnViewInternal.DOp.toPredicate("4)", false).test(3));
    }
}