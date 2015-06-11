package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
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
public class ChronicleMapKeyValueStoreTest {
    public static final String NAME = "chronmapkvstoretests";
    private static Map<String, Factor> map;
    private static KeyValueStore mapU;

    @BeforeClass
    public static void createMap() throws IOException {
        //Delete any files from the last run
        Files.deleteIfExists(Paths.get(OS.TMP, NAME));

        resetChassis();
        Function<Bytes, Wire> writeType = TextWire::new;

        addWrappingRule(MapView.class, "map directly to KeyValueStore", VanillaMapView::new, KeyValueStore.class);

        addLeafRule(KeyValueStore.class, "KVS is Chronicle Map", (context, asset) ->
                new ChronicleMapKeyValueStore(context.wireType(writeType).basePath(OS.TMP), asset));

        map = acquireMap(NAME, String.class, Factor.class);
        mapU = ((VanillaMapView) map).underlying();
        assertEquals(ChronicleMapKeyValueStore.class, mapU.getClass());

        //just in case it hasn't been cleared up last time
        map.clear();

    }

    @AfterClass
    public static void tearDown() throws Exception {
        mapU.close();
    }

    @Test
    public void test() throws Exception {
        AtomicInteger success = new AtomicInteger();
        MapEventListener<String, Factor> listener = new MapEventListener<String, Factor>() {
            @Override
            public void update(String key, Factor oldValue, Factor newValue) {
                System.out.println("Updated { key: " + key + ", oldValue: " + oldValue + ", value: " + newValue + " }");
                success.set(-1000);
            }

            @Override
            public void insert(String key, Factor value) {
                System.out.println("Inserted { key: " + key + ", value: " + value + " }");
                success.incrementAndGet();
            }

            @Override
            public void remove(String key, Factor oldValue) {
                System.out.println("Removed { key: " + key + ", value: " + oldValue + " }");
                success.set(-100);
            }
        };

        Asset asset = getAsset(NAME);
        registerSubscriber(NAME, MapEvent.class, e -> e.apply(listener));
        //ChronicleMapKeyValueStore sbskvStore = asset.acquireView(ChronicleMapKeyValueStore.class);
        //sbskvStore.registerSubscriber(MapEvent.class, (x) ->
        //        System.out.println(x), "");

        Factor factor = new Factor();
        factor.setAccountNumber("xyz");
        map.put("testA", factor);
        assertEquals(1, map.size());
        assertEquals("xyz", map.get("testA").getAccountNumber());

        expectedSuccess(success, 1);
        success.set(0);

        factor.setAccountNumber("abc");
        map.put("testA", factor);

        expectedSuccess(success, -1000);
        success.set(0);

        map.remove("testA");

        expectedSuccess(success, -100);
        success.set(0);
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