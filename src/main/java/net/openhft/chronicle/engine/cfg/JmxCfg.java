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

package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by peter on 26/08/15.
 */
public class JmxCfg implements Installable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxCfg.class);
    private boolean enabled;

    @Override
    public void install(String path, AssetTree assetTree) {
        if (enabled) {
            LOGGER.info("Enabling JMX for " + assetTree);
            assetTree.enableManagement();
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "enabled").bool(b -> enabled = b);
    }

    @Override
    public String toString() {
        return "JmxCfg{" +
                "enabled=" + enabled +
                '}';
    }
}
