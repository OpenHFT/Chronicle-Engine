package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
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
        root.addLeafRule(HostIdentifier.class, "host id holder", (rc, context) -> new HostIdentifier(hostId));
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
}
