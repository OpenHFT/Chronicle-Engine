package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.query.QueueConfig;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetRuleProvider;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.WireType;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EngineInstanceTest {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(MyRuleProvider.class, "MyRuleProvider");
    }

    @Rule
    public ShutdownHooks hooks = new ShutdownHooks();

    @Test
    public void testEngineInstanceLoads() throws IOException {
        TCPRegistry.createServerSocketChannelFor("localhost9090", "localhost9091");
        try (VanillaAssetTree assetTree = hooks.addCloseable(EngineInstance.engineMain(1, "engine.yaml"))) {
            assertNotNull(assetTree);
            Asset asset = assetTree.getAsset("/queue/queue1");
            assertNotNull(asset);
            QueueView view = asset.getView(QueueView.class);
            assertNotNull(view);
            QueueConfig qc = asset.getView(QueueConfig.class);
            assertNotNull(qc);
            assertTrue(WireType.BINARY.equals(qc.wireType()));
        }
        TCPRegistry.reset();
    }

    @Test
    public void testEngineInstanceLoadsWithCustomRuleProvider() throws IOException {
        TCPRegistry.createServerSocketChannelFor("localhost9090", "localhost9091");
        try (VanillaAssetTree assetTree = hooks.addCloseable(EngineInstance.engineMain(1, "engine2.yaml"))) {
            assertNotNull(assetTree);
            Asset asset = assetTree.getAsset("/queue/queue1");
            assertNotNull(asset);
            QueueView view = asset.getView(QueueView.class);
            assertNotNull(view);
            QueueConfig qc = asset.getView(QueueConfig.class);
            assertNotNull(qc);
            assertTrue(WireType.BINARY.equals(qc.wireType()));
            assertTrue(assetTree.root().getRuleProvider() instanceof MyRuleProvider);
        }
        TCPRegistry.reset();
    }

    public static class MyRuleProvider extends VanillaAssetRuleProvider {

    }

}

