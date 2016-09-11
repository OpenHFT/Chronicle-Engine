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

package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.ThreadMonitoringTest;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    public static Collection<Object[]> data() {
        // ensure the aliases have been loaded.
        RequestContext.requestContext();

        final List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{WireType.BINARY});
        list.add(new Object[]{WireType.TEXT});
        return list;
    }

    @Test
    public void test() {

        final Bytes b = Bytes.elasticByteBuffer();
        final Wire wire = wireType.apply(b);
        assert wire.startUse();

        final Operation operation = new Operation(Operation.OperationType.FILTER, (SerializablePredicate) o -> true);
        wire.writeDocument(false, w -> w.write(() -> "operation").object(operation));

        wire.readDocument(null, w -> {
            final Object object = w.read(() -> "operation").object(Object.class);

            Assert.assertNotNull(object);
        });
    }
}