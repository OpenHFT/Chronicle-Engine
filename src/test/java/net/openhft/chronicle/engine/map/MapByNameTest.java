package net.openhft.chronicle.engine.map;

import junit.framework.Assert;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapWireConnectionHub;
import net.openhft.chronicle.map.EngineMap;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */
public class MapByNameTest {

    private static byte LOCAL_IDENTIFIER = (byte) 1;
    private static int SERVER_PORT = 8085;


    @Test
    public void testSizeUsingLocalMap() throws IOException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class,
                CharSequence.class)) {

            // remote client map

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());


            // local server map
            EngineMap test2 = new EngineMap("test",
                    CharSequence.class,
                    Integer.class,
                    r.serverEndpoint.mapWireConnectionHub(), TextWire.class);

            Assert.assertEquals(1, test2.size());
        }
    }


    @Test
    public void testGetUsingLocalMap() throws IOException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class,
                CharSequence.class)) {

            // remote client map

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());


            // local server map
            final EngineMap<Integer, CharSequence> localServerMap = new EngineMap<>(
                    "test",
                    Integer.class,
                    CharSequence.class,
                    r.serverEndpoint.mapWireConnectionHub(),
                    TextWire.class);

            Assert.assertEquals("hello", localServerMap.get(1));

        }
    }


    @Test
    public void testPutAndGetMapOnlyOnServer() throws IOException {


        try (MapWireConnectionHub mapWireConnectionHub = new MapWireConnectionHub(
                LOCAL_IDENTIFIER,
                SERVER_PORT)) {

            final EngineMap<Integer, CharSequence> localServerMap = new EngineMap<>(
                    "test",
                    Integer.class,
                    CharSequence.class,
                    mapWireConnectionHub,
                    TextWire.class);

            Assert.assertEquals(null, localServerMap.put(1, "hello"));

            Assert.assertEquals("hello", localServerMap.get(1));
            mapWireConnectionHub.close();

        }
    }


}
