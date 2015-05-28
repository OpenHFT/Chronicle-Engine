package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.AssetNotFoundException;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.Session;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LocalSession implements Session {
    final Session baseSession;
    final LocalAsset root;

    public LocalSession(Session baseSession) {
        this.baseSession = baseSession;
        root = new LocalAsset(this, "", null, baseSession.getAsset(""));
    }

    @NotNull
    @Override
    public <A> Asset acquireAsset(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        Asset asset = getAsset(name);
        if (asset == null) {
            int endIndex = name.lastIndexOf('/');
            String parent = endIndex > 0 ? name.substring(0, endIndex) : "";
            String name0 = name.substring(endIndex + 1);
            LocalAsset asset3 = (LocalAsset) getAssetOrANFE(parent);
            Asset asset2 = baseSession.acquireAsset(name, assetClass, class1, class2);
            LocalAsset ret = new LocalAsset(this, name, asset3, asset2);
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
