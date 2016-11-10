package net.openhft.chronicle.engine.net.openhft.chronicle.engine.api;

import net.openhft.chronicle.engine.api.column.ColumnView;
import net.openhft.chronicle.engine.api.column.MapColumnView;
import net.openhft.chronicle.engine.api.column.Row;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Rob Austin.
 */
@RunWith(value = Parameterized.class)
public class TestColumnView {

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

    public TestColumnView(Boolean isRemote) throws Exception {

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

        YamlLogging.setAll(true);
        assetTree.acquireMap("/my/data", String.class, String.class).put("hello", "world");

        final Asset asset = assetTree.acquireAsset("/my/data");
        final MapColumnView columnView = asset.acquireView(MapColumnView.class);
        final Iterator<? extends Row> iterator = columnView.iterator(new ColumnView.SortedFilter());

        final ArrayList<Row> dataCollector = new ArrayList<>();
        iterator.forEachRemaining(dataCollector::add);
        Assert.assertEquals(1, dataCollector.size());

    }


}
