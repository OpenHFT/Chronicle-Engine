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
import net.openhft.chronicle.engine.fs.ConfigurationFS;
import net.openhft.chronicle.engine.map.InsertedEvent;
import net.openhft.chronicle.engine.map.RemovedEvent;
import net.openhft.chronicle.engine.map.UpdatedEvent;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.threads.api.EventLoop;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

import static net.openhft.chronicle.core.pool.ClassAliasPool.CLASS_ALIASES;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAssetTree implements AssetTree {
    static {
        CLASS_ALIASES.addAlias(AddedAssetEvent.class,
                ExistingAssetEvent.class,
                RemovedAssetEvent.class,
                InsertedEvent.class,
                UpdatedEvent.class,
                RemovedEvent.class);
    }

    final VanillaAsset root = new VanillaAsset(null, "");

    public VanillaAssetTree() {

    }

    public VanillaAssetTree(int hostId) {
        root.addView(HostIdentifier.class, new HostIdentifier((byte) hostId));
    }

    @NotNull
    public VanillaAssetTree forTesting() {
        return forTesting(true);
    }

    @NotNull
    public VanillaAssetTree forTesting(boolean daemon) {
        root.forTesting(daemon);
        return this;
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess(String hostname, int port) {
        root.forRemoteAccess(hostname, port);
        return this;
    }

    @NotNull
    @Override
    public Asset acquireAsset(@NotNull String fullName) throws AssetNotFoundException {
        if (fullName.startsWith("/"))
            fullName = fullName.substring(1);
        return fullName.isEmpty() ? root : root.acquireAsset(fullName);
    }

    @Nullable
    @Override
    public Asset getAsset(@NotNull String fullName) {
        if (fullName.startsWith("/"))
            fullName = fullName.substring(1);
        return fullName.isEmpty() ? root : root.getAsset(fullName);
    }

    @NotNull
    @Override
    public Asset root() {
        return root;
    }

    @Override
    public void close() {

        // ensure that the event loop get shutdown first
        try {
            EventLoop view = root().findView(EventLoop.class);
            if (view != null)
                view.close();
        } catch (Exception e) {
            //
        }

        root.close();
    }

    @NotNull
    public AssetTree withConfig(String etcDir, String baseDir) {
        Threads.withThreadGroup(root.getView(ThreadGroup.class), () -> {
            new ConfigurationFS("/etc", etcDir, baseDir).install(baseDir, this);
            return null;
        });
        return this;
    }

    @NotNull
    @Override
    public String toString() {
        return "tree-" + Optional.ofNullable(root.getView(HostIdentifier.class)).map(HostIdentifier::hostId).orElseGet(() -> (byte) 0);
    }
}
