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

package net.openhft.chronicle.engine.eg;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * Created by peter on 17/08/15.
 */
public class ServerMain {
    private static ServerEndpoint endpoint;

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Price.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(PriceUpdater.class);
    }

    public static void main(String[] args) throws IOException {

        YamlLogging.showServerReads(true);
        YamlLogging.showServerWrites(true);
        @NotNull AssetTree serverTree = new VanillaAssetTree().forServer(false);
        endpoint = new ServerEndpoint("localhost:9090", serverTree);
    }
}
