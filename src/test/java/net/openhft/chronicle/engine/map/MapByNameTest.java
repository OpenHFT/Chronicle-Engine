package net.openhft.chronicle.engine.map;

import junit.framework.Assert;
import net.openhft.chronicle.engine.map.WireRemoteStatelessMapClientTest.RemoteMapSupplier;
import net.openhft.chronicle.hash.ChronicleHashInstanceBuilder;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.MapWireConnectionHub;
import net.openhft.chronicle.map.WireMap;
import net.openhft.chronicle.wire.TextWire;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Supplier;

import static net.openhft.chronicle.map.ChronicleMapBuilder.of;
import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */
public class MapByNameTest {

    @Test
    public void testSizeUsingLocalMap() throws IOException {

        try (RemoteMapSupplier<Integer, CharSequence> r = new RemoteMapSupplier<>(Integer.class,
                CharSequence.class)) {

            // remote client map

            ChronicleMap<Integer, CharSequence> clientMap = r.get();
            clientMap.put(1, "hello");
            assertEquals(1, clientMap.size());


            // local server map
            WireMap test2 = new WireMap("test",
                    CharSequence.class,
                    Integer.class,
                    r.serverEndpoint().mapWireConnectionHub(), TextWire.class);

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
            final MapWireConnectionHub mapWireConnectionHub = r.serverEndpoint().mapWireConnectionHub();
            final WireMap<Integer, CharSequence> localServerMap = new
                    WireMap<>(
                    "test",
                    CharSequence.class,
                    Integer.class,
                    mapWireConnectionHub,
                    TextWire.class);

            Assert.assertEquals("hello", localServerMap.get(1));

        }
    }


    @Test
    public void testPutAndGetMapOnlyOnServer() throws IOException {

        final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<byte[], byte[]>>> mapFactory
                = () -> of(byte[].class, byte[].class).instance();

        final Supplier<ChronicleHashInstanceBuilder<ChronicleMap<String, Integer>>> channelNameToIdFactory
                = () -> of(String.class, Integer.class).instance();

        MapWireConnectionHub mapWireConnectionHub =
                new MapWireConnectionHub(
                        mapFactory,
                        channelNameToIdFactory,
                        (byte) 1,
                        8085);

        final WireMap<Integer, CharSequence> localServerMap = new WireMap<>(
                "test",
                CharSequence.class,
                Integer.class,
                mapWireConnectionHub,
                TextWire.class);


        Assert.assertEquals(null, localServerMap.put(1, "hello"));

        Assert.assertEquals("hello", localServerMap.get(1));


    }


}
