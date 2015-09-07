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

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.cfg.Installable;
import net.openhft.chronicle.engine.pubsub.SubscriptionStat;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;


/**
 * Created by peter on 26/08/15.
 */
public class MonitorCfg implements Installable, Marshallable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitorCfg.class);
    private boolean enabled;
    private boolean debug;

    @Override
    public MonitorCfg install(String path, AssetTree assetTree) throws IOException, URISyntaxException {
        if (enabled) {
            LOGGER.info("Enabling Monitoring for " + assetTree);
            MapView map = assetTree.acquireMap("/proc/subscriptions", String.class, SubscriptionStat.class);
            map.size();
        }
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "enabled").bool(this, (o, b) -> o.enabled = b);
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(() -> "enabled").bool(enabled);
    }

    @Override
    public String toString() {
        return "MonitorCfg{" +
                "enabled=" + enabled +
                '}';
    }
}
