/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import java.net.URISyntaxException;

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

    public static void main(@NotNull String... args) throws IOException, InterruptedException, URISyntaxException {
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

        serverEndpoint = new ServerEndpoint("*:" + PORT, assetTree, WIRE_TYPE);

        if (MSG_DUMP) {
            LOGGER.info("Enabling message logging");
            YamlLogging.setAll(true);
        }

        LOGGER.info("Server port seems to be " + PORT);
    }
}
