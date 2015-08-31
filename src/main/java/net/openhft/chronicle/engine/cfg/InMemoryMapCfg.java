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

import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by peter on 26/08/15.
 */
public class InMemoryMapCfg implements Installable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryMapCfg.class);
    private Class keyType, valueType;
    private boolean putReturnsNull, removeReturnsNull;
    private String importFile;

    @Override
    public Void install(String path, AssetTree assetTree) throws IOException {
        String uri = path + "?putReturnsNull=" + putReturnsNull + "&removeReturnsNull=" + removeReturnsNull;
        MapView mapView = assetTree.acquireMap(uri, keyType, valueType);
        if (importFile != null) {
            Wire wire = Wire.fromFile(importFile);
            StringBuilder keyStr = new StringBuilder();
            while (wire.hasMore()) {
                Object value = wire.readEventName(keyStr).object(valueType);
                Object key = ObjectUtils.convertTo(keyType, keyStr);
                mapView.put(key, value);
            }
        }
        LOGGER.info("Added InMemoryMap " + path + ", size: " + mapView.size());
        return null;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "keyType").typeLiteral(this, (o, c) -> o.keyType = c)
                .read(() -> "valueType").typeLiteral(this, (o, c) -> o.valueType = c)
                .read(() -> "putReturnsNull").bool(this, (o, e) -> o.putReturnsNull = e)
                .read(() -> "removeReturnsNull").bool(this, (o, e) -> o.removeReturnsNull = e);
        if (wire.hasMore())
            wire.read(() -> "import").text(this, (o, s) -> o.importFile = s);
    }

    @Override
    public String toString() {
        return "InMemoryMapCfg{" +
                "keyType=" + keyType +
                ", valueType=" + valueType +
                ", putReturnsNull=" + putReturnsNull +
                ", removeReturnsNull=" + removeReturnsNull +
                '}';
    }
}
