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

package net.openhft.chronicle.engine.eg;

import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.YamlLogging;

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
        AssetTree serverTree = new VanillaAssetTree().forServer(false);
        endpoint = new ServerEndpoint("localhost:9090", serverTree);
    }
}
