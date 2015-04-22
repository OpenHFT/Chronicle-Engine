package net.openhft.chronicle.engine.map;

import junit.framework.Assert;
import net.openhft.chronicle.engine.map.WireRemoteStatelessMapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapWireConnectionHubByName;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */
public class MapByNameTest {

    @Test
    public void test() throws IOException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class,
                CharSequence.class)) {

            // remote client map

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());


            // local server map
            MapWireConnectionHubByName test2 = new MapWireConnectionHubByName("test",
                    CharSequence.class,
                    Integer.class,
                    r.serverEndpoint().mapWireConnectionHub());

            Assert.assertEquals(1, test2.size());
        }
    }

}
