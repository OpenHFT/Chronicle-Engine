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

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.AuthenticatedKeyValueStore;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by peter on 26/08/15.
 */
public class FilePerKeyMapCfg implements Installable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilePerKeyMapCfg.class);
    private Class keyType, valueType;
    private boolean putReturnsNull, removeReturnsNull;
    private String compression;
    private String diskPath;

    @Nullable
    @Override
    public Void install(@NotNull String path, @NotNull AssetTree assetTree) throws IOException {
        @NotNull Asset asset = assetTree.acquireAsset(path);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        @NotNull String uri = path + "?putReturnsNull=" + putReturnsNull + "&removeReturnsNull=" + removeReturnsNull;
        @NotNull RequestContext rc = RequestContext.requestContext(uri);
        asset.addView(AuthenticatedKeyValueStore.class, new FilePerKeyValueStore(rc, asset));
        @NotNull MapView mapView = assetTree.acquireMap(uri, keyType, valueType);
        LOGGER.info("Added FilePerKeyMap " + path + ", size: " + mapView.size());
        return null;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        wire.read(() -> "keyType").typeLiteral(this, (o, c) -> o.keyType = c)
                .read(() -> "valueType").typeLiteral(this, (o, c) -> o.valueType = c)
                .read(() -> "compression").text(this, (o, c) -> o.compression = c)
                .read(() -> "putReturnsNull").bool(this, (o, e) -> o.putReturnsNull = e)
                .read(() -> "removeReturnsNull").bool(this, (o, e) -> o.removeReturnsNull = e)
                .read(() -> "diskPath").text(this, (o, s) -> o.diskPath = s);
    }

    @NotNull
    @Override
    public String toString() {
        return "FilePerKeyMapCfg{" +
                "keyType=" + keyType +
                ", valueType=" + valueType +
                ", putReturnsNull=" + putReturnsNull +
                ", removeReturnsNull=" + removeReturnsNull +
                ", compression='" + compression + '\'' +
                ", diskPath='" + diskPath + '\'' +
                '}';
    }
}
