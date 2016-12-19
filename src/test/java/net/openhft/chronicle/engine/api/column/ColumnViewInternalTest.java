package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.map.MapWrappingColumnView;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Created by rob on 19/12/2016.
 */
public class ColumnViewInternalTest {

    @Test
    public void test() {

        MapWrappingColumnView cv = ObjectUtils.newInstance(MapWrappingColumnView.class);

        ArrayList results = new ArrayList();
        int[] numbers = {1, 2, 3, 4};
        for (Number n : numbers) {
            if (cv.toRange(n, "(2,4]"))
                results.add(n);
        }

        Assert.assertEquals("[3, 4]", results.toString());
    }

    @Test
    public void testRange() {
        Assert.assertTrue(ColumnViewInternal.DOp.toRange(3, "4]", false));
        Assert.assertTrue(ColumnViewInternal.DOp.toRange(3, "3]", false));
        Assert.assertFalse(ColumnViewInternal.DOp.toRange(3, "3)", false));
        Assert.assertTrue(ColumnViewInternal.DOp.toRange(3, "4)", false));
    }
}