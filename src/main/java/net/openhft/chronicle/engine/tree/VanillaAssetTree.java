package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.Asset;
import net.openhft.chronicle.engine.api.AssetNotFoundException;
import net.openhft.chronicle.engine.api.AssetTree;
import net.openhft.chronicle.engine.api.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAssetTree implements AssetTree {
    final VanillaAsset root = new VanillaAsset(null, "");

    public VanillaAssetTree() {

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
    public <A> Asset acquireAsset(Class<A> assetClass, @NotNull RequestContext context) throws AssetNotFoundException {
        String fullName = context.fullName();
        return fullName.isEmpty() || fullName.equals("/") ? root : root.acquireAsset(context, fullName);
    }

    @Nullable
    @Override
    public Asset getAsset(@NotNull String fullName) {
        return fullName.isEmpty() || fullName.equals("/") ? root : root.getAsset(fullName);
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
