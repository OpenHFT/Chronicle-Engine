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
@Deprecated
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
        wire.read(() -> "spec").text(this, (o, s) -> o.spec = s)
                .read(() -> "name").text(this, (o, s) -> o.name = s)
                .read(() -> "cluster").text(this, (o, s) -> o.cluster = s)
                .read(() -> "maxEntries").int64(this, (o, e) -> o.maxEntries = e)
                .read(() -> "averageValueSize").int32(this, (o, e) -> o.averageValueSize = e)
                .read(() -> "putReturnsNull").bool(this, (o, b) -> o.putReturnsNull = b)
                .read(() -> "removeReturnsNull").bool(this, (o, b) -> o.removeReturnsNull = b);
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
        @NotNull Asset asset = assetTree.acquireAsset(name);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        asset.addLeafRule(KeyValueStore.class, "use Chronicle Map", this);
        this.basePath = baseDir;
    }

    @NotNull
    @Override
    public KeyValueStore create(@NotNull final RequestContext requestContext, @NotNull final Asset asset) throws AssetNotFoundException {
        return new ChronicleMapKeyValueStore(requestContext.basePath(basePath), asset);
    }
}
