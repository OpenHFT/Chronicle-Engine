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
