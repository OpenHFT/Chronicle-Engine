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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.AssetTreeStats;
import net.openhft.chronicle.engine.fs.ConfigurationFS;
import net.openhft.chronicle.engine.map.InsertedEvent;
import net.openhft.chronicle.engine.map.RemovedEvent;
import net.openhft.chronicle.engine.map.UpdatedEvent;
import net.openhft.chronicle.engine.map.remote.*;
import net.openhft.chronicle.network.VanillaSessionDetails;
import net.openhft.chronicle.network.connection.ClientConnectionMonitor;
import net.openhft.chronicle.threads.Threads;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.function.Function;

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
                MapFunction.class,
                MapUpdate.class,
                RemovedEvent.class,
                KeyFunctionPair.class,
                KeyValueFunctionTuple.class,
                KeyValuesTuple.class,
                KeyValuePair.class);
    }

    @NotNull
    final VanillaAsset root;

    public VanillaAssetTree() {
        this("");
    }

    public VanillaAssetTree(@Nullable String name) {
        root = new VanillaAsset(null, name == null ? "" : name);
    }

    public VanillaAssetTree(int hostId) {
        this();
        root.addView(HostIdentifier.class, new HostIdentifier((byte) hostId));
    }

    @Override
    public AssetTreeStats getUsageStats() {
        AssetTreeStats ats = new AssetTreeStats();
        root.getUsageStats(ats);
        return ats;
    }

    @NotNull
    public VanillaAssetTree forTesting() {
        return forTesting(true);
    }

    @NotNull
    public VanillaAssetTree forTesting(boolean daemon) {
        root.forServer(daemon);
        return this;
    }

    @NotNull
    public VanillaAssetTree forServer() {
        return forServer(true);
    }

    @NotNull
    public VanillaAssetTree forServer(boolean daemon) {
        root.forServer(daemon);
        return this;
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess(String hostPortDescription, @NotNull Function<Bytes, Wire> wire) {
        root.forRemoteAccess(new String[]{hostPortDescription}, wire, clientSession(), null);
        return this;
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess(@NotNull String[] hostPortDescription, @NotNull Function<Bytes, Wire> wire) {
        root.forRemoteAccess(hostPortDescription, wire, clientSession(), null);
        return this;
    }

    /**
     * creates an asset tree that connects to a remove server via tcp/ip
     *
     * @param hostPortDescription     the primary host and other failover hosts
     * @param wire                    the type of wire
     * @param clientConnectionMonitor used to monitor client failover
     * @return an instance of VanillaAssetTree
     */
    @NotNull
    public VanillaAssetTree forRemoteAccess(@NotNull String[] hostPortDescription,
                                            @NotNull Function<Bytes, Wire> wire,
                                            @Nullable ClientConnectionMonitor clientConnectionMonitor) {

        if (clientConnectionMonitor != null)
            root.viewMap.put(ClientConnectionMonitor.class, clientConnectionMonitor);

        root.forRemoteAccess(hostPortDescription, wire, clientSession(), clientConnectionMonitor);
        return this;
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess(String hostPortDescription) {
        return forRemoteAccess(hostPortDescription, WireType.BINARY);
    }

    @NotNull
    private VanillaSessionDetails clientSession() {
        final VanillaSessionDetails sessionDetails = new VanillaSessionDetails();
        sessionDetails.setUserId(System.getProperty("user.name"));
        return sessionDetails;
    }


    @NotNull
    @Override
    public Asset acquireAsset(@NotNull String fullName) {
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
    public VanillaAsset root() {
        return root;
    }

    @Override
    public void close() {
        root.notifyClosing();
        Jvm.pause(100);

        // ensure that the event loop get shutdown first
        try {
            EventLoop view = root().findView(EventLoop.class);
            Closeable.closeQuietly(view);
        } catch (Exception e) {
            //
        }

        root.close();
    }

    @NotNull
    @Deprecated
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
