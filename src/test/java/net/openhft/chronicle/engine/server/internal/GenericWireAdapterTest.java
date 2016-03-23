/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author Rob Austin.
 */
@RunWith(Parameterized.class)
public class GenericWireAdapterTest {

    public static final Double EXPECTED = 1.23;
    private final WireType wireType;

    /**
     * @param wireType the type of the wire
     */
    public GenericWireAdapterTest(@NotNull WireType wireType) {
        this.wireType = wireType;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {WireType.TEXT},
                {WireType.BINARY}
        });
    }

    @Test
    public void testValueToWire() throws Exception {
        final GenericWireAdapter<String, Double> genericWireAdapter = new GenericWireAdapter(
                String.class, Double.class);

        final Bytes b = Bytes.elasticByteBuffer();
        final Wire wire = wireType.apply(b);

        wire.writeDocument(false, w -> {
            genericWireAdapter.valueToWire().accept(wire.getValueOut(), EXPECTED);

        });

        System.out.println("----------------------------------");
        System.out.println(Wires.fromSizePrefixedBlobs(wire.bytes()));

        wire.readDocument(null, w -> {

            final Double actual = genericWireAdapter.wireToValue().apply(wire.getValueIn());

          //  System.out.println(actual);
            Assert.assertEquals(EXPECTED, actual);
        });


    }


}