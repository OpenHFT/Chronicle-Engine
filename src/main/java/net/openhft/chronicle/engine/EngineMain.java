/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.management.mbean.ChronicleConfig;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.cfg.*;
import net.openhft.chronicle.engine.tree.AssetRuleProvider;
import net.openhft.chronicle.engine.tree.VanillaAssetRuleProvider;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.TextWire;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * This engine main uses a configuration file
 */
public class EngineMain {
    static final Logger LOGGER = LoggerFactory.getLogger(EngineMain.class);
    static final int HOST_ID = Integer.getInteger("engine.hostId", 0);

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(EngineCfg.class,
                JmxCfg.class,
                ServerCfg.class,
                ClustersCfg.class,
                InMemoryMapCfg.class,
                FilePerKeyMapCfg.class,
                ChronicleMapCfg.class,
                MonitorCfg.class,
                VanillaAssetRuleProvider.class);
    }

    public static void main(@NotNull String[] args) throws IOException {
        ChronicleConfig.init();

        @NotNull String name = args.length > 0 ? args[0] : resolveConfigurationFile();
        @NotNull TextWire yaml = TextWire.fromFile(name);
        @NotNull Installable installable = (Installable) yaml.readObject();
        @NotNull AssetRuleProvider ruleProvider = getRuleProvider(installable);
        @NotNull AssetTree assetTree = new VanillaAssetTree(HOST_ID, ruleProvider).forServer(false);
      //  assetTree.registerSubscriber("", TopologicalEvent.class, e -> LOGGER.info("Tree change "
        //        + e));
        try {
            installable.install("/", assetTree);
            LOGGER.info("Engine started");

        } catch (Exception e) {
            LOGGER.error("Error starting a component, stopping", e);
            assetTree.close();
        }
    }

    private static AssetRuleProvider getRuleProvider(Installable installable) {
        if (installable instanceof EngineCfg) {
            EngineCfg engineCfg = (EngineCfg) installable;
            return engineCfg.getRuleProvider();
        }
        return new VanillaAssetRuleProvider();
    }

    @NotNull
    private static String resolveConfigurationFile() {
        final File expectedDemoConfig = new File(OS.USER_DIR, "demo/src/main/resources/engine.yaml");
        if (expectedDemoConfig.exists()) {
            return expectedDemoConfig.getAbsolutePath();
        }
        return "engine.yaml";
    }
}
