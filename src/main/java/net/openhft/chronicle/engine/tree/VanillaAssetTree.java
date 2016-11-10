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

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.threads.EventLoop;
import net.openhft.chronicle.engine.api.column.ColumnViewInternal.SortedFilter;
import net.openhft.chronicle.engine.api.column.ColumnViewIterator;
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
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.function.Consumer;

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
                KeyValuePair.class,
                SortedFilter.class,
                ColumnViewIterator.class);
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
        return forServer(daemon);
    }

    @NotNull
    public VanillaAssetTree forTesting(boolean daemon, boolean binding) {
        return forServer(daemon, binding);
    }

    @NotNull
    public VanillaAssetTree forServer() {
        return forServer(true);
    }

    @NotNull
    public VanillaAssetTree forServer(boolean daemon, boolean binding) {
        final HostIdentifier view = root.getView(HostIdentifier.class);
        final int hostId = view == null ? 1 : view.hostId();
        root.forServer(daemon, (String uri) -> VanillaAsset.master(uri, hostId), binding);
        return this;
    }

    @NotNull
    public VanillaAssetTree forServer(boolean daemon) {
        return forServer(daemon, false);
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess(String hostPortDescription, @NotNull WireType wire) {
        return forRemoteAccess(new String[]{hostPortDescription}, wire);
    }

    @NotNull
    public VanillaAssetTree forRemoteAccess(@NotNull String[] hostPortDescription,
                                            @NotNull WireType wire) {
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
                                            @NotNull WireType wire,
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
        sessionDetails.userId(System.getProperty("user.name"));
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
        Jvm.pause(50);

        // ensure that the event loop get shutdown first
        EventLoop view = root().findView(EventLoop.class);
        Closeable.closeQuietly(view);

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

    public VanillaAssetTree forRemoteAccess(String serverAddress, WireType wireType, Consumer<Throwable> t) {
        return forRemoteAccess(serverAddress, wireType);
    }
}
