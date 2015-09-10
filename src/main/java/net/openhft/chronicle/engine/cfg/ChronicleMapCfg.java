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

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.map.VanillaMapView;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by daniel on 26/08/15.
 */
public class ChronicleMapCfg implements Installable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChronicleMapCfg.class);
    private Class keyType, valueType;
    private boolean putReturnsNull, removeReturnsNull;
    private String compression;
    private String diskPath;
    private long entries=-1;
    private double averageSize=-1;

    @Override
    public Void install(String path, AssetTree assetTree) throws IOException {
        Asset asset = assetTree.acquireAsset(path);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        RequestContext rc = RequestContext.requestContext(path);
        rc.basePath(diskPath)
                .putReturnsNull(putReturnsNull)
                .removeReturnsNull(removeReturnsNull);

        if(entries != -1)rc.entries(entries);
        if(averageSize != -1)rc.averageValueSize(averageSize);


        ChronicleMapKeyValueStore chronicleMapKeyValueStore = new ChronicleMapKeyValueStore(rc, asset);
        asset.addView(KeyValueStore.class, chronicleMapKeyValueStore);
        asset.addView(MapView.class, new VanillaMapView(rc, asset, chronicleMapKeyValueStore));

        return null;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "diskPath").text(this, (o, c) -> o.diskPath = c)
                .read(() -> "keyType").typeLiteral(this, (o, c) -> o.keyType = c)
                .read(() -> "valueType").typeLiteral(this, (o, c) -> o.valueType = c)
                .read(() -> "compression").text(this, (o, c) -> o.compression = c)
                .read(() -> "putReturnsNull").bool(this, (o, e) -> o.putReturnsNull = e)
                .read(() -> "removeReturnsNull").bool(this, (o, e) -> o.removeReturnsNull = e)
                .read(() -> "entries").int64(this, (o, e) -> o.entries = e)
                .read(() -> "averageSize").float64(this, (o, e) -> o.averageSize = e);
    }

    @Override
    public String toString() {
        return "ChronicleMapCfg{" +
                "keyType=" + keyType +
                ", valueType=" + valueType +
                ", putReturnsNull=" + putReturnsNull +
                ", removeReturnsNull=" + removeReturnsNull +
                ", compression='" + compression + '\'' +
                '}';
    }
}
