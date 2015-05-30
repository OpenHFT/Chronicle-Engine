package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.*;
import org.jetbrains.annotations.Nullable;

public class LocalSession implements Session {
    final Session baseSession;
    final LocalAsset root;

    public LocalSession(Session baseSession) {
        this.baseSession = baseSession;
        root = new LocalAsset(this, "", null, baseSession.getAsset(""));
    }

    @Override
    public <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws AssetNotFoundException {
        Asset asset = getAsset(context.fullName());
        if (asset == null) {
            String parent = context.namePath();
            String name0 = context.name();
            LocalAsset asset3 = (LocalAsset) getAssetOrANFE(parent);
            Asset asset2 = baseSession.acquireAsset(assetClass, context);
            LocalAsset ret = new LocalAsset(this, name0, asset3, asset2);
            asset3.add(name0, ret);
            return ret;
        }
        return asset;

    }


    @Nullable
    @Override
    public Asset getAsset(String fullName) {
        return root.getAsset(fullName);
    }

    @Override
    public Asset add(String fullName, Assetted resource) {
        return root.add(fullName, resource);
    }


    @Override
    public void close() {
        root.close();
    }
}
