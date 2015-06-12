package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.Factor;
import net.openhft.chronicle.wire.BinaryWire;
import net.openhft.chronicle.wire.TextWire;
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

    @Test
    public void testMarshalling() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, String> insertedEvent = InsertedEvent.of("asset", "key", "name");
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshalling2() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, Factor> insertedEvent = InsertedEvent.of("asset", "key", new Factor());
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }


    @Test
    @Ignore("TODO Fix")
    public void testMarshalling3a() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, BytesStore> insertedEvent = InsertedEvent.of("asset", "key", BytesStore.wrap("£Hello World".getBytes()));
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    @Ignore("TODO Fix")
    public void testMarshalling3() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, BytesStore> insertedEvent = InsertedEvent.of("asset", "key", BytesStore.wrap("Hello World".getBytes()));
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) textWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshallingB() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, String> insertedEvent = InsertedEvent.of("asset", "key", "name");
        BinaryWire binaryWire = new BinaryWire(bytes);
        binaryWire.write(() -> "reply").typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) binaryWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }

    @Test
    public void testMarshalling2B() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, Factor> insertedEvent = InsertedEvent.of("asset", "key", new Factor());
        BinaryWire binaryWire = new BinaryWire(bytes);
        binaryWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) binaryWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }


    @Test
    public void testMarshalling3B() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, BytesStore> insertedEvent = InsertedEvent.of("asset", "key", BytesStore.wrap("Hello World".getBytes()));
        BinaryWire binaryWire = new BinaryWire(bytes);
        binaryWire.write(() -> "reply")
                .typedMarshallable(insertedEvent);
        bytes.flip();
        System.out.println("text: " + bytes);
        InsertedEvent ie = (InsertedEvent) binaryWire.read(() -> "reply").typedMarshallable();
        assertEquals(insertedEvent, ie);
    }
}