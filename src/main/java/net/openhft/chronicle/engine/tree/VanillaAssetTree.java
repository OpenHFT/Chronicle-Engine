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

    public VanillaAssetTree forTesting() {
        root.forTesting();
        return this;
    }

    public VanillaAssetTree forRemoteAccess() {
        root.forRemoteAccess();
        return this;
    }

    @NotNull
    @Override
    public <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws AssetNotFoundException {
        String name = context.fullUri();
        return name.isEmpty() || name.equals("/") ? root : root.acquireAsset(name);
    }

    @Nullable
    @Override
    public Asset getAsset(String fullName) {
        return fullName.isEmpty() || fullName.equals("/") ? root : root.getAsset(fullName);
    }

    @Override
    public void close() {
        root.close();
    }

    public void viewTypeLayersOn(Class viewType, String description, Class underlyingType) {
        root.viewTypeLayersOn(viewType, description, underlyingType);
    }
}
