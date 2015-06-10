package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.Assetted;
import net.openhft.chronicle.engine.api.RequestContext;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.map.StringBytesStoreKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.map.VanillaStringMarshallableKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaSubscriptionKeyValueStore;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static net.openhft.chronicle.engine.Chassis.*;
import static org.junit.Assert.assertEquals;

/**
 * JUnit test class to support
 */
//@Ignore("todo fix test, getting more events on Windows - JIRA https://higherfrequencytrading.atlassian.net/browse/CE-63")
public class FilePerKeyValueStoreTest {
    public static final String NAME = "fileperkvstoretests";
    private static Map<String, TestMarshallable> map;

    @BeforeClass
    public static void createMap() throws IOException {
        //String TMP = "/tmp";
        String TMP = System.getProperty("java.io.tmpdir");

        resetChassis();
        Function<Bytes, Wire> writeType = TextWire::new;
        enableTranslatingValuesToBytesStore();

        addLeafRule(KeyValueStore.class, "FilePer Key",
                (context, asset) -> new FilePerKeyValueStore(context.basePath(TMP).wireType(writeType), asset));

        map = acquireMap(NAME, String.class, TestMarshallable.class);
        KeyValueStore mapU = ((VanillaMapView) map).underlying();
        assertEquals(VanillaStringMarshallableKeyValueStore.class, mapU.getClass());
        assertEquals(VanillaSubscriptionKeyValueStore.class, mapU.underlying().getClass());
        assertEquals(FilePerKeyValueStore.class, ((Assetted) mapU.underlying()).underlying().getClass());

        //just in case it hasn't been cleared up last time
        map.clear();
    }

    @Test
    public void test() throws InterruptedException {
        TestMarshallable tm = new TestMarshallable("testing1", "testing2",
                new Nested(Arrays.asList(2.3, 4.5, 6.7, 8.9)));

        AtomicInteger success = new AtomicInteger();
        MapEventListener<String, TestMarshallable> listener = new MapEventListener<String, TestMarshallable>() {
            @Override
            public void update(@NotNull String key, @NotNull TestMarshallable oldValue, @NotNull TestMarshallable newValue) {
                assert key != null;
                assert oldValue != null;
                assert newValue != null;
                System.out.println("Updated { key: " + key + ", oldValue: " + oldValue + ", value: " + newValue + " }");
                success.set(-1000);
            }

            @Override
            public void insert(@NotNull String key, @NotNull TestMarshallable value) {
                assert key != null;
                assert value != null;
                System.out.println("Inserted { key: " + key + ", value: " + value + " }");
                success.incrementAndGet();
            }

            @Override
            public void remove(@NotNull String key, TestMarshallable oldValue) {
                assert key != null;
                System.out.println("Removed { key: " + key + ", value: " + oldValue + " }");
                success.set(-100);
            }
        };
        Asset asset = getAsset(NAME);
        registerSubscriber(NAME, MapEvent.class, e -> e.apply(listener));
        StringBytesStoreKeyValueStore sbskvStore = asset.getView(StringBytesStoreKeyValueStore.class);
        sbskvStore.subscription(true).registerSubscriber(RequestContext.requestContext("").type(MapEvent.class), System.out::println);

        map.put("testA", tm);

        assertEquals(1, map.size());
        assertEquals("testing1", map.get("testA").getS1());
        assertEquals(4.5, map.get("testA").getNested().getListDouble().get(1), 0);

        for (int i = 0; i < 20; i++) {
            if (success.get() == 1)
                break;
            TimeUnit.MILLISECONDS.sleep(200);
        }
        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(1, success.get());
    }

    static class TestMarshallable implements Marshallable {
        private String s1, s2;
        private Nested nested;

        public TestMarshallable() {
            nested = new Nested();
        }

        public TestMarshallable(String s1, String s2, Nested nested) {
            this.s1 = s1;
            this.s2 = s2;
            this.nested = nested;
        }

        public Nested getNested() {
            return nested;
        }

        public void setNested(Nested nested) {
            this.nested = nested;
        }

        public String getS1() {
            return s1;
        }

        public void setS1(String s1) {
            this.s1 = s1;
        }

        public String getS2() {
            return s2;
        }

        public void setS2(String s2) {
            this.s2 = s2;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wireIn) throws IllegalStateException {
            setS1(wireIn.read(TestKey.S1).text());
            setS2(wireIn.read(TestKey.S2).text());
            wireIn.read(TestKey.nested).marshallable(nested);
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wireOut) {
            wireOut.write(TestKey.S1).text(getS1());
            wireOut.write(TestKey.S2).text(getS2());
            wireOut.write(TestKey.nested).marshallable(nested);
        }

        @NotNull
        @Override
        public String toString() {
            return "TestMarshallable{" +
                    "s1='" + s1 + '\'' +
                    ", s2='" + s2 + '\'' +
                    ", nested=" + nested +
                    '}';
        }

        private enum TestKey implements WireKey {
            S1, S2, nested
        }
    }

    static class Nested implements Marshallable {
        List<Double> listDouble;

        public Nested() {
            listDouble = new ArrayList<>();
        }

        public Nested(List<Double> listDouble) {
            this.listDouble = listDouble;
        }

        public List<Double> getListDouble() {
            return listDouble;
        }

        public void setListDouble(List<Double> listDouble) {
            this.listDouble = listDouble;
        }

        @Override
        public void readMarshallable(@NotNull WireIn wireIn) throws IllegalStateException {
            listDouble.clear();
            wireIn.read(TestKey.listDouble).sequence(v -> {
                while (v.hasNextSequenceItem()) {
                    v.float64(listDouble::add);
                }
            });
        }

        @Override
        public void writeMarshallable(@NotNull WireOut wireOut) {
            wireOut.write(TestKey.listDouble).sequence(v ->
                            listDouble.stream().forEach(v::float64)
            );
        }

        @NotNull
        @Override
        public String toString() {
            return "Nested{" +
                    "listDouble=" + listDouble +
                    '}';
        }

        private enum TestKey implements WireKey {
            listDouble;
        }
    }
}
