/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.engine.Factor;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 12/06/15.
 */
public class InsertedEventTest {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(InsertedEvent.class, Factor.class);
    }


    private ThreadDump threadDump;

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Test
    public void testMarshalling() {
        YamlLogging.setAll(false);
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, String> insertedEvent = InsertedEvent.of("asset", "key", "name",false);
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshalling2() {
        YamlLogging.setAll(false);
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, Factor> insertedEvent = InsertedEvent.of("asset", "key", new Factor(),false);
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    @Ignore("TODO Fix")
    public void testMarshalling3a() {
        YamlLogging.setAll(false);
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, BytesStore> insertedEvent = InsertedEvent.of("asset", "key", BytesStore.wrap("£Hello World".getBytes()),false);
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshalling3() {
        YamlLogging.setAll(false);
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, BytesStore> insertedEvent = InsertedEvent.of("asset", "key", BytesStore.wrap("Hello World".getBytes()),false);
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshallingB() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, String> insertedEvent = InsertedEvent.of("asset", "key", "name",false);
        BinaryWire binaryWire = new BinaryWire(bytes);
        binaryWire.write(() -> "reply").typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = binaryWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshalling2B() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, Factor> insertedEvent = InsertedEvent.of("asset", "key", new Factor(),false);
        BinaryWire binaryWire = new BinaryWire(bytes);
        binaryWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = binaryWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshalling3B() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, BytesStore> insertedEvent = InsertedEvent.of("asset", "key", BytesStore.wrap("Hello World".getBytes()),false);
        BinaryWire binaryWire = new BinaryWire(bytes);
        binaryWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        System.out.println("text: " + bytes);
        InsertedEvent ie = binaryWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }
}