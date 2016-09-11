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

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.After;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by peter on 17/08/15.
 */

/*
Run ServerMain first

Prints
GBPEUR: Price{instrument='GBPEUR', bidPrice=0.71023, bidQuantity=1000000.0, askPrice=0.71024, askQuantity=2000000.0}
GBPEUR: Price{instrument='GBPEUR', bidPrice=0.71023, bidQuantity=1000000.0, askPrice=0.71024, askQuantity=2000000.0}
GBPEUR: Price{instrument='GBPEUR', bidPrice=0.71023, bidQuantity=1000000.0, askPrice=0.71027, askQuantity=1.2E7}
GBPEUR: Price{instrument='GBPEUR', bidPrice=0.71023, bidQuantity=1000000.0, askPrice=0.71025, askQuantity=2000000.0}
GBPUSD: Price{instrument='GBPEUR', bidPrice=0.64153, bidQuantity=3000000.0, askPrice=0.64154, askQuantity=2000000.0}
GBPUSD: Price{instrument='GBPEUR', bidPrice=0.64153, bidQuantity=3000000.0, askPrice=0.64154, askQuantity=3000000.0}
GBPUSD: Price{instrument='GBPEUR', bidPrice=0.64153, bidQuantity=4000000.0, askPrice=0.64154, askQuantity=3000000.0}
GBPUSD: Price{instrument='GBPEUR', bidPrice=0.64151, bidQuantity=4000000.0, askPrice=0.64154, askQuantity=3000000.0}
mid: 0.641515
GBPUSD: Price{instrument='GBPEUR', bidPrice=0.64149, bidQuantity=4000000.0, askPrice=0.64154, askQuantity=3000000.0}
 */
public class ClientTwoMain {
    private static AtomicReference<Throwable> t = new AtomicReference();

    static {
        ClassAliasPool.CLASS_ALIASES.addAlias(Price.class);
        ClassAliasPool.CLASS_ALIASES.addAlias(PriceUpdater.class);
    }

    public static void main(String[] args) {
        YamlLogging.setAll(false);
        VanillaAssetTree assetTree = new VanillaAssetTree().forRemoteAccess("localhost:9090",
                WireType.TEXT);

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

        while (true) {
            Jvm.pause(100);
        }
    }

    @After
    public void afterMethod() {
        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);
    }
}
