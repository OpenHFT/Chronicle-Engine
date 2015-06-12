package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.Factor;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by peter on 12/06/15.
 */
public class InsertedEventTest {

    @Test
    public void testMarshalling() {
        Bytes bytes = Bytes.elasticByteBuffer();
        InsertedEvent<String, String> insertedEvent = InsertedEvent.of("asset", "key", "name");
        TextWire textWire = new TextWire(bytes);
        textWire.write(() -> "reply").typedMarshallable(insertedEvent);
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

}