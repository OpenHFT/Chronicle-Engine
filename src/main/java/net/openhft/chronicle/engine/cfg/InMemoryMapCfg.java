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
    private String compression;
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
                .read(() -> "compression").text(this, (o, c) -> o.compression = c)
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
                ", compression='" + compression + '\'' +
                ", importFile='" + importFile + '\'' +
                '}';
    }
}
