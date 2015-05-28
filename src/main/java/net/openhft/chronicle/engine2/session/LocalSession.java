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
    public <A> Asset acquireAsset(RequestContext request) throws AssetNotFoundException {
        Asset asset = getAsset(request.fullName());
        if (asset == null) {
            String parent = request.namePath();
            String name0 = request.name();
            LocalAsset asset3 = (LocalAsset) getAssetOrANFE(parent);
            Asset asset2 = baseSession.acquireAsset(request);
            LocalAsset ret = new LocalAsset(this, name0, asset3, asset2);
            asset3.add(name0, ret);
            return ret;
        }
        return asset;

    }

    @Nullable
    @Override
    public Asset getAsset(String name) {
        return root.getAsset(name);
    }


    @Override
    public Asset add(String name, Assetted resource) {
        return root.add(name, resource);
    }

    @Override
    public void close() {
        root.close();
    }


    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        root.registerView(iClass, interceptor);
    }

}
