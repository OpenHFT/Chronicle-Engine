package net.openhft.chronicle.engine.map;

import junit.framework.Assert;
import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.map.MapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.EngineMap;
import net.openhft.chronicle.map.MapWireConnectionHub;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */
public class MapByNameTest {

    private static byte LOCAL_IDENTIFIER = (byte) 1;
    private static int SERVER_PORT = 8085;


    @Ignore
    @Test
    public void testSizeUsingLocalMap() throws IOException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class,
                CharSequence.class,
                new ChronicleEngine())) {

            // remote client map

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());


            // local server map

            EngineMap test2 = new EngineMap<>(
                    EngineMap.underlyingMap("test", r.serverEndpoint.mapWireConnectionHub(), 10),
                    CharSequence.class,
                    Integer.class,
                    TextWire.class);


            Assert.assertEquals(1, test2.size());
        }
    }

    @Ignore
    @Test
    public void testGetUsingLocalMap() throws IOException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class,
                CharSequence.class,
                new ChronicleEngine())) {

            // remote client map

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());


            // local server map
            EngineMap localServerMap = new EngineMap<>(
                    EngineMap.underlyingMap("test", r.serverEndpoint.mapWireConnectionHub(), 10),
                    Integer.class,
                    CharSequence.class,
                    TextWire.class);

            Assert.assertEquals("hello", localServerMap.get(1));

        }
    }


    @Test
    public void testPutAndGetMapOnlyOnServer() throws IOException {


        try (MapWireConnectionHub mapWireConnectionHub = new MapWireConnectionHub(
                LOCAL_IDENTIFIER,
                SERVER_PORT)) {

            EngineMap localServerMap = new EngineMap<>(
                    EngineMap.underlyingMap("test", mapWireConnectionHub, 10),
                    Integer.class,
                    CharSequence.class,
                    TextWire.class);


            Assert.assertEquals(null, localServerMap.put(1, "hello"));

            Assert.assertEquals("hello", localServerMap.get(1));
            mapWireConnectionHub.close();

        }
    }


}
