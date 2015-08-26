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
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by peter on 26/08/15.
 */
public class EngineCfg implements Installable {
    static final Logger LOGGER = LoggerFactory.getLogger(EngineCfg.class);
    final Map<String, Installable> installableMap = new LinkedHashMap<>();

    @Override
    public void install(String path, AssetTree assetTree) throws Exception {
        LOGGER.info("Building Engine " + assetTree);
        for (Map.Entry<String, Installable> entry : installableMap.entrySet()) {
            LOGGER.info("Installing " + entry.getKey() + ": " + entry.getValue());
            entry.getValue().install(entry.getKey(), assetTree);
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        readMarshallable("", wire);
    }

    private void readMarshallable(String path, WireIn wire) {
        StringBuilder name = new StringBuilder();
        while (wire.hasMore()) {
            ValueIn in = wire.read(name);
            StringBuilder type = new StringBuilder();
            long pos = wire.bytes().readPosition();
            in.type(type);
            String path2 = path + "/" + name;
            if (type.length() == 0) {
                in.marshallable(w -> this.readMarshallable(path2, w));
            } else {
                wire.bytes().readPosition(pos);
                Object o = in.typedMarshallable();
                installableMap.put(path2, (Installable) o);
            }
        }
    }
}
