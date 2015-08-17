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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;

/**
 * Created by peter on 17/08/15.
 */
public class ClientTwoMain {
    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Price.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(PriceUpdater.class);
    }

    public static void main(String[] args) {
        YamlLogging.setAll(true);
        VanillaAssetTree assetTree = new VanillaAssetTree().forRemoteAccess("localhost:9090", WireType.TEXT);

        MapView<String, Price> map = assetTree.acquireMap("/fx", String.class, Price.class);
        map.put("GBPEUR", new Price("GBPEUR", 0.71023, 1e6, 0.71024, 2e6));
        map.put("GBPUSD", new Price("GBPEUR", 0.64153, 3e6, 0.64154, 2e6));
        map.put("EURUSD", new Price("EURUSD", 0.90296, 5e6, 0.90299, 4e6));
        map.put("CHFUSD", new Price("CHFUSD", 0.97692, 2e6, 0.97696, 11e6));

        assetTree.registerSubscriber("/fx/GBPEUR", Price.class, s -> System.out.println("GBPEUR: " + s));

        map.put("GBPEUR", new Price("GBPEUR", 0.71023, 1e6, 0.71024, 2e6));
        map.put("GBPEUR", new Price("GBPEUR", 0.71023, 1e6, 0.71027, 12e6));
        map.put("GBPEUR", new Price("GBPEUR", 0.71023, 1e6, 0.71025, 2e6));

        assetTree.registerSubscriber("/fx/GBPUSD", Price.class, s -> System.out.println("GBPUSD: " + s));

        map.asyncUpdateKey("GBPUSD", p -> {
            p.askQuantity += 1e6;
            return p;
        });
        map.asyncUpdateKey("GBPUSD", p -> {
            p.bidQuantity += 1e6;
            return p;
        });
        map.asyncUpdateKey("GBPUSD", PriceUpdater.SET_BID_PRICE, 0.64151);

        Object gbpusd = map.syncUpdateKey("GBPUSD", PriceUpdater.SET_BID_PRICE, 0.64149, (p, a) -> Maths.round6((p.bidPrice + p.askPrice) / 2), null);
        // TODO this shouldn't be needed.
        double mid = Double.parseDouble((String) gbpusd);
        System.out.println("mid: " + mid);

        Jvm.pause(1000);
    }
}
