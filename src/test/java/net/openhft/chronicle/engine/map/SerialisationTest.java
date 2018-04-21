package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Marshallable;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Jerry Shea on 15/01/18.
 */
@Ignore
public class SerialisationTest {
    @Test
    public void textWire() {
        CMap2EngineReplicator.VanillaReplicatedEntry dto = new CMap2EngineReplicator.VanillaReplicatedEntry(), dto2 = null;
        byte[] binaryData = new byte[]{1, 2, 3, 4};
        dto.key(Bytes.wrapForRead(binaryData));
        dto.timestamp(123L);
        String cs = dto.toString();
        System.out.println(cs);
        dto2 = Marshallable.fromString(cs);
        assertEquals(cs, dto2.toString());
    }
}
