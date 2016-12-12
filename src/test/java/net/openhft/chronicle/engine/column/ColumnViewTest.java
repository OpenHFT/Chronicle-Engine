package net.openhft.chronicle.engine.column;

import net.openhft.chronicle.engine.api.column.ClosableIterator;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal.SortedFilter;
import net.openhft.chronicle.engine.api.column.MapColumnView;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class ColumnViewTest {


    @NotNull
    @Rule
    public TestName name = new TestName();
    String methodName = "";

    private final VanillaAssetTree assetTree;
    private final ServerEndpoint serverEndpoint;


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Boolean[][]{
                {false}, {true}
        });
    }

    public ColumnViewTest(Boolean isRemote) throws Exception {

        if (isRemote) {
            VanillaAssetTree assetTree0 = new VanillaAssetTree().forTesting();

            String hostPortDescription = "SimpleQueueViewTest-methodName" + methodName;
            TCPRegistry.createServerSocketChannelFor(hostPortDescription);
            serverEndpoint = new ServerEndpoint(hostPortDescription, assetTree0);

            final VanillaAssetTree client = new VanillaAssetTree();
            assetTree = client.forRemoteAccess(hostPortDescription, WireType.BINARY);

        } else {
            assetTree = (new VanillaAssetTree(1)).forTesting();
            serverEndpoint = null;
        }

    }


    @Test
    public void test() {
        //YamlLogging.setAll(true);
        assetTree.acquireMap("/my/data", String.class, String.class).put("hello", "world");

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        final Iterator<? extends Row> iterator = columnView.iterator(new SortedFilter());

        final ArrayList<Row> dataCollector = new ArrayList<>();
        iterator.forEachRemaining(dataCollector::add);
        Assert.assertEquals(1, dataCollector.size());
    }


    @Test
    public void testColumnMapView2ChunksEachChunk300Entries() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<String, String> map = assetTree.acquireMap("/my/data", String.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put("hello" + i, "world");
        }


        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        final Iterator<? extends Row> iterator = columnView.iterator(new SortedFilter());

        final ArrayList<Row> dataCollector = new ArrayList<>();
        iterator.forEachRemaining(dataCollector::add);
        Assert.assertEquals(size, dataCollector.size());
    }


    @Test
    public void testFilteredRequestColumnView() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<String, String> map = assetTree.acquireMap("/my/data", String.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put("hello" + i, "world");
        }

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        SortedFilter sortedFilter = new SortedFilter();
        sortedFilter.marshableFilters.add(new ColumnViewInternal.MarshableFilter("key", "hello0"));

        final Iterator<? extends Row> iterator = columnView.iterator(sortedFilter);

        final ArrayList<Row> dataCollector = new ArrayList<>();
        iterator.forEachRemaining(dataCollector::add);
        Assert.assertEquals(1, dataCollector.size());
    }

    @Test
    public void testSortByKeyForColumnMapView() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<Integer, String> map = assetTree.acquireMap("/my/data", Integer.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put(i, "world");
        }

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        SortedFilter sortedFilter = new SortedFilter();
        sortedFilter.marshableOrderBy.add(new ColumnViewInternal.MarshableOrderBy("key"));

        final Iterator<? extends Row> iterator = columnView.iterator(sortedFilter);


        for (int i = 0; i < size; i++) {
            Assert.assertTrue(iterator.hasNext());
            Row row = iterator.next();
            Assert.assertEquals(i, row.get(0));
            Assert.assertEquals("world", row.get(1));
        }

    }


    @Test
    public void testSortByKeyForColumnMapViewWithAnotherIteratorSentFirst() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<Integer, String> map = assetTree.acquireMap("/my/data", Integer.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put(i, "world");
        }

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        {
            try (final ClosableIterator<? extends Row> iterator = columnView.iterator(new SortedFilter())) {

            }

        }

        final SortedFilter sortedFilter = new SortedFilter();
        sortedFilter.marshableOrderBy.add(new ColumnViewInternal.MarshableOrderBy("key"));

        try (final ClosableIterator<? extends Row> iterator = columnView.iterator(sortedFilter)) {
            for (int i = 0; i < size; i++) {
                Assert.assertTrue(iterator.hasNext());
                Row row = iterator.next();
                Assert.assertEquals(i, row.get(0));
                Assert.assertEquals("world", row.get(1));
            }
        }

        System.out.println("finished");
    }


    @Test
    public void testMapColumnViewRowCount() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<Integer, String> map = assetTree.acquireMap("/my/data", Integer.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put(i, "world");
        }

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        Assert.assertEquals(size, columnView.rowCount(new SortedFilter()));
    }


    @Test
    public void testCanDeleteRows() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<Integer, String> map = assetTree.acquireMap("/my/data", Integer.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put(i, "world");
        }

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        Assert.assertEquals(true, columnView.canDeleteRows());
    }


    @Test
    public void testContainRowWithKey() {

        final int size = 600;

        //YamlLogging.setAll(true);
        MapView<Integer, String> map = assetTree.acquireMap("/my/data", Integer.class, String.class);
        for (int i = 0; i < size; i++) {
            map.put(i, "world");
        }

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);

        Assert.assertEquals(true, columnView.containsRowWithKey(Collections.singletonList(12)));

    }

}
