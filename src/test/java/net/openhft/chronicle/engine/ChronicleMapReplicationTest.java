package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static net.openhft.chronicle.engine.Chassis.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by daniel on 28/05/15.
 */
public class ChronicleMapReplicationTest {
    public static final String NAME = "chronmapkvstoretests";
    private static Map<String, Factor> map;
    private static KeyValueStore mapU;

    @BeforeClass
    public static void createMap() throws IOException {
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TMP, NAME));

        resetChassis();
        Function<Bytes, Wire> writeType = TextWire::new;

        viewTypeLayersOn(MapView.class, "map directly to KeyValueStore", KeyValueStore.class);

        registerFactory("", KeyValueStore.class, (context, asset, underlyingSupplier) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).basePath(OS.TMP), asset));

        map = acquireMap(NAME, String.class, Factor.class);
        mapU = ((VanillaMapView) map).underlying();
        assertEquals(ChronicleMapKeyValueStore.class, mapU.getClass());

        //just in case it hasn't been cleared up last time
        map.clear();

    }

    @AfterClass
    public static void tearDown() throws Exception{
        mapU.close();
    }

    @Test
    public void test() throws Exception{
      // todo
    }

    private void expectedSuccess(@NotNull AtomicInteger success, int expected) {
        for (int i = 0; i < 20; i++) {
            if (success.get() == expected)
                break;
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        assertEquals(expected, success.get());
    }
}