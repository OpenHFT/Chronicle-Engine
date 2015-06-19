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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.fs.ConfigurationFS;
import net.openhft.chronicle.engine.map.InsertedEvent;
import net.openhft.chronicle.engine.map.RemovedEvent;
import net.openhft.chronicle.engine.map.UpdatedEvent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAssetTree implements AssetTree {
    final VanillaAsset root = new VanillaAsset(null, "");

    static {
        CLASS_ALIASES.addAlias(AddedAssetEvent.class,
                ExistingAssetEvent.class,
                RemovedAssetEvent.class,
                InsertedEvent.class,
                UpdatedEvent.class,
                RemovedEvent.class);
    }

    public VanillaAssetTree() {

    }

    public VanillaAssetTree(int hostId) {
        root.addLeafRule(HostIdentifier.class, "host id holder", (rc, context) -> new HostIdentifier((byte) hostId));
    }

    @NotNull
    public VanillaAssetTree forTesting() {
        root.forTesting();
        return this;
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess() {
        root.forRemoteAccess();
        return this;
    }

    @NotNull
    @Override
    public Asset acquireAsset(Class assetClass, @NotNull RequestContext context) throws AssetNotFoundException {
        String fullName = context.fullName();
        if (fullName.startsWith("/"))
            fullName = fullName.substring(1);
        return fullName.isEmpty() ? root : root.acquireAsset(context, fullName);
    }

    @Nullable
    @Override
    public Asset getAsset(@NotNull String fullName) {
        if (fullName.startsWith("/"))
            fullName = fullName.substring(1);
        return fullName.isEmpty() ? root : root.getAsset(fullName);
    }

    @Override
    public Asset root() {
        return root;
    }

    @Override
    public void close() {
        root.close();
    }

    public AssetTree withConfig(String etcDir, String baseDir) {
        new ConfigurationFS("/etc", etcDir, baseDir).install(baseDir, this);
        return this;
    }
}
