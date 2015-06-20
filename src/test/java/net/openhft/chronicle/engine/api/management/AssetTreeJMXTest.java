package net.openhft.chronicle.engine.api.management;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by pct25 on 6/18/2015.
 */
public class AssetTreeJMXTest {

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


    /**
     * Provide the test case for add numbers of map into AssetTree
     *
     * @param number the numbers of map to add into AssetTree
     */
    private void addMapIntoTree(int number) throws InterruptedException {

        AssetTree tree = new VanillaAssetTree().forTesting();
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

        AssetTree tree = new VanillaAssetTree().forTesting();
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
