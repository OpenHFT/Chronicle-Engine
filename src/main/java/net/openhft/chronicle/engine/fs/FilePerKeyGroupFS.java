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
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.map.FilePerKeyValueStore;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

/**
 * Created by peter on 12/06/15.
 */
@Deprecated
public class FilePerKeyGroupFS implements Marshallable, MountPoint {
    private String spec;
    private String name;
    private Class valueType;
    private boolean recurse;

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
                .read(() -> "valueType").typeLiteral(this, (o, t) -> o.valueType = t)
                .read(() -> "recurse").bool(this, (o, b) -> o.recurse = b);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "spec").text(spec)
                .write(() -> "name").text(name)
                .write(() -> "valueType").typeLiteral(CLASS_ALIASES.nameFor(valueType))
                .write(() -> "recurse").bool(recurse);
    }

    @Override
    public void install(String baseDir, @NotNull AssetTree assetTree) {
        RequestContext context = RequestContext.requestContext(name).basePath(baseDir + "/" + spec).recurse(this.recurse).keyType(String.class);
        Asset asset = assetTree.acquireAsset(name);
        ((VanillaAsset) asset).enableTranslatingValuesToBytesStore();
        asset.registerView(KeyValueStore.class, new FilePerKeyValueStore(context, asset));
    }
}
