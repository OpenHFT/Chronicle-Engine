package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.query.QueueConfig;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class EngineInstanceTest {

    @Test
    public void testEngineInstanceLoads() throws IOException {
        TCPRegistry.createServerSocketChannelFor("localhost9090", "localhost9091");
        try (VanillaAssetTree assetTree = EngineInstance.engineMain(1, "engine.yaml")) {
            Assert.assertNotNull(assetTree);
            Asset asset = assetTree.getAsset("/queue/queue1");
            Assert.assertNotNull(asset);
            QueueView view = asset.getView(QueueView.class);
            Assert.assertNotNull(view);
            QueueConfig qc = asset.getView(QueueConfig.class);
            Assert.assertNotNull(qc);
            Assert.assertNotNull(WireType.BINARY.equals(qc.wireType()));
        }
        TCPRegistry.reset();
    }

}

