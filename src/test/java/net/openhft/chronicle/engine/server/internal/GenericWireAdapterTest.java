/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
    private ThreadDump threadDump;

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

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Test
    public void testValueToWire() {
        final GenericWireAdapter<String, Double> genericWireAdapter = new GenericWireAdapter(
                String.class, Double.class);

        final Bytes b = Bytes.elasticByteBuffer();
        final Wire wire = wireType.apply(b);
        assert wire.startUse();

        wire.writeDocument(false, w -> {
            genericWireAdapter.valueToWire().accept(wire.getValueOut(), EXPECTED);

        });

        System.out.println("----------------------------------");
        System.out.println(Wires.fromSizePrefixedBlobs(wire));

        wire.readDocument(null, w -> {

            final Double actual = genericWireAdapter.wireToValue().apply(wire.getValueIn());

          //  System.out.println(actual);
            Assert.assertEquals(EXPECTED, actual);
        });

    }
}
