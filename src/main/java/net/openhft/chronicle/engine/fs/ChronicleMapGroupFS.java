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

package net.openhft.chronicle.engine.fs;

import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.map.ChronicleMapKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 12/06/15.
 */
public class ChronicleMapGroupFS implements Marshallable, MountPoint, LeafViewFactory<KeyValueStore> {
    String spec, name, cluster;
    int averageValueSize;
    Boolean putReturnsNull, removeReturnsNull;
    private long maxEntries;
    private transient String basePath;

    @Override
    public String spec() {
        return spec;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "spec").text(s -> spec = s)
                .read(() -> "name").text(s -> name = s)
                .read(() -> "cluster").text(s -> cluster = s)
                .read(() -> "maxEntries").int64(e -> maxEntries = e)
                .read(() -> "averageValueSize").int32(e -> averageValueSize = e)
                .read(() -> "putReturnsNull").bool(e -> putReturnsNull = e)
                .read(() -> "removeReturnsNull").bool(e -> removeReturnsNull = e);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "spec").text(spec)
                .write(() -> "name").text(name)
                .write(() -> "cluster").text(cluster)
                .write(() -> "maxEntries").int64(maxEntries)
                .write(() -> "averageValueSize").int32(averageValueSize)
                .write(() -> "putReturnsNull").bool(putReturnsNull)
                .write(() -> "removeReturnsNull").bool(removeReturnsNull);
    }

    @Override
    public void install(String baseDir, @NotNull AssetTree assetTree) {
        Asset asset = assetTree.acquireAsset(name);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        asset.addLeafRule(KeyValueStore.class, "use Chronicle Map", this);
        this.basePath = baseDir;
    }

    @NotNull
    @Override
    public KeyValueStore create(@NotNull final RequestContext requestContext, final Asset asset) throws AssetNotFoundException {
        return new ChronicleMapKeyValueStore(requestContext.basePath(basePath), asset);
    }
}
