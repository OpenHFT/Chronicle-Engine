import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.CMap2EngineReplicator;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static net.openhft.chronicle.hash.replication.SingleChronicleHashReplication.builder;

/**
 * @author Rob Austin.
 */
public class ReUseSamePersistedFile {


    @Test
    public void testName() throws Exception {

        File file = File.createTempFile("chron", ".map");

        final CMap2EngineReplicator engineReplicationLangBytesConsumer = new CMap2EngineReplicator(RequestContext.requestContext(), null);
        try (ChronicleMap<String, String> map =
                     ChronicleMapBuilder.of(String.class, String.class)
                             .replication(builder()
                                     .engineReplication(engineReplicationLangBytesConsumer)
                                     .createWithId((byte) 1))
                             .createPersistedTo(file)) {
            map.put("hello1", "world");
            Assert.assertEquals(1, map.size());
        }

        try (ChronicleMap<String, String> map =
                     ChronicleMapBuilder.of(String.class, String.class)
                             .replication(builder()
                                     .engineReplication(engineReplicationLangBytesConsumer)
                                     .createWithId((byte) 1))
                             .createPersistedTo(file)) {
            map.put("hello2", "world");
            Assert.assertEquals(2, map.size());
        }
    }


}

