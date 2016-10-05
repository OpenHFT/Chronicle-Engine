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
import net.openhft.chronicle.engine.api.management.mbean.ChronicleConfig;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.TopologicalEvent;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This engine main creates an empty tree configured from the command line.
 *
 * @author peter
 * @author andre
 */
public class SimpleEngineMain {

    static final Logger LOGGER = LoggerFactory.getLogger(SimpleEngineMain.class);
    static final int HOST_ID = Integer.getInteger("engine.hostId", 0);
    static final boolean JMX = Boolean.getBoolean("engine.jmx");
    static final boolean PERSIST = Boolean.getBoolean("engine.persist");
    static final boolean MSG_DUMP = Boolean.getBoolean("engine.messages.dump");
    static final int PORT = Integer.getInteger("engine.port", 8088);
    static final WireType WIRE_TYPE = WireType.valueOf(System.getProperty("engine.wireType", "BINARY"));

    static ServerEndpoint serverEndpoint;

    public static void main(@NotNull String... args) throws IOException {
        VanillaAssetTree assetTree = tree();


        MapView<String, String> mapView = assetTree.acquireMap("/my/map", String.class, String.class);
        mapView.put("hello", "world");
    }

    @NotNull
    public static VanillaAssetTree tree() throws IOException {
        ChronicleConfig.init();
        VanillaAssetTree assetTree = new VanillaAssetTree(HOST_ID).forTesting(false);
        if (JMX)
            assetTree.enableManagement();

        assetTree.registerSubscriber("", TopologicalEvent.class, e -> LOGGER.info("Tree change ", e));
        if (PERSIST) {
            LOGGER.info("Persistence enabled");
            assetTree.root().addWrappingRule(MapView.class, "map directly to KeyValueStore",
                    VanillaMapView::new, KeyValueStore.class);
            assetTree.root().addLeafRule(KeyValueStore.class, "use Chronicle Map", (context, asset) ->
                    new ChronicleMapKeyValueStore(context.basePath(OS.TARGET), asset));
        }

        serverEndpoint = new ServerEndpoint("*:" + PORT, assetTree);

        if (MSG_DUMP) {
            LOGGER.info("Enabling message logging");
            YamlLogging.setAll(true);
        }

        LOGGER.info("Server port seems to be " + PORT);
        return assetTree;
    }
}
