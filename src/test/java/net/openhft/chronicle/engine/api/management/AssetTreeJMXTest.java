package net.openhft.chronicle.engine.api.management;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.TextWire;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static net.openhft.chronicle.bytes.NativeBytes.nativeBytes;
import static org.junit.Assert.assertTrue;

/**
 * Created by pct25 on 6/18/2015.
 */
@Ignore("Long running test")
public class AssetTreeJMXTest {

    private static AtomicReference<Throwable> t = new AtomicReference();

    @Test
    public void testAssetUpdateEvery10Sec() throws InterruptedException {
        testAssetUpdate(10000);
    }

    @Test
    public void testAssetUpdateEvery1Sec() throws InterruptedException {
        testAssetUpdate(1000);
    }

    @Test
    public void testAssetUpdateEvery100MilliSec() throws InterruptedException {
        testAssetUpdate(100);
    }

    @Ignore(value = "javax.management.InstanceNotFoundException")
    @Test
    public void testAssetUpdateEvery10MilliSec() throws InterruptedException {
        testAssetUpdate(10);
    }

    @Test
    public void add1ThousandMapIntoTree() throws InterruptedException {
        addMapIntoTree(1000);
    }

    @Test
    public void add1LakhMapIntoTree() throws InterruptedException {
        addMapIntoTree(100000);
    }

    @Ignore("java.lang.OutOfMemoryError: GC overhead limit exceeded")
    @Test
    public void add1MillionMapIntoTree() throws InterruptedException {
        addMapIntoTree(1000000);
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }

    @Test
    public void addStringValuesMapIntoTree(){
        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        tree.enableManagement();
        ConcurrentMap<String, String> map = tree.acquireMap("group/map", String.class, String.class);
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789ABCDEGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0132456789");
        Jvm.pause(100000);
    }

    @Test
    public void addDoubleValuesMapIntoTree(){
        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        tree.enableManagement();
        ConcurrentMap<Double, Double> map = tree.acquireMap("group/map", Double.class, Double.class);
        map.put(1.1, 1.1);
        map.put(1.01, 1.01);
        map.put(1.001, 1.001);
        map.put(1.0001, 1.0001);
        Jvm.pause(20000);
    }

    @Test
    public void addIntegerValuesMapIntoTree(){
        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        tree.enableManagement();
        ConcurrentMap<Integer, Integer> map = tree.acquireMap("group/map", Integer.class, Integer.class);
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        Jvm.pause(20000);
    }

    @Ignore("todo add assertions")
    @Test
    public void addMarshallableValuesMapIntoTree(){
        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        tree.enableManagement();

        Marshallable m = new MyTypes();

        Bytes bytes = nativeBytes();
        assertTrue(bytes.isElastic());
        TextWire wire = new TextWire(bytes);
        m.writeMarshallable(wire);
        m.readMarshallable(wire);

        ConcurrentMap<String, Marshallable> map = tree.acquireMap("group/map", String.class, Marshallable.class);
        map.put("1",m);
        map.put("2",m);
        map.put("3",m);
        Jvm.pause(200);
    }

    /**
     * Provide the test case for add numbers of map into AssetTree
     *
     * @param number the numbers of map to add into AssetTree
     */
    private void addMapIntoTree(int number) throws InterruptedException {

        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        tree.enableManagement();

        for (int i = 1; i <= number ; i++) {
            ConcurrentMap<String, String> map1 = tree.acquireMap("group/map"+i, String.class, String.class);
            map1.put("key1", "value1");
        }
        Jvm.pause(10000);
    }

    /**
     * Provide the test case for add and remove numbers of map into AssetTree with time interval of milliseconds
     * It will add and remove map for 1 hour.
     *
     * @param milliSeconds the interval in milliSeconds to add and remove map into AssetTree
     */
    private void testAssetUpdate(long milliSeconds) throws InterruptedException {

        AssetTree tree = new VanillaAssetTree().forTesting(x -> t.compareAndSet(null, x));
        tree.enableManagement();

        long timeToStop = System.currentTimeMillis() + 3600000;  //3600000 = 60*60*1000 milliseconds = 1 Hour
        int count = 0;
        while (System.currentTimeMillis()<=timeToStop){
            ConcurrentMap<String, String> map1 = tree.acquireMap("group/map"+count, String.class, String.class);
            map1.put("key1", "value1");
            Jvm.pause(milliSeconds);
            tree.root().getAsset("group").removeChild("map"+count);
            count++;
        }
    }
}
