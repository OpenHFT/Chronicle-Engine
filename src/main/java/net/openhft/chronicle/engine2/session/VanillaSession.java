package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStoreFactory;
import net.openhft.chronicle.engine2.api.map.MapViewFactory;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStoreSupplier;
import net.openhft.chronicle.engine2.map.MapView;
import net.openhft.chronicle.engine2.map.VanillaKeyValueStore;
import net.openhft.chronicle.engine2.map.VanillaSubscriptionKeyValueStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSession implements Session {
    final VanillaAsset root = new VanillaAsset(null, "", null);

    public VanillaSession() {
        root.registerInterceptor(SubscriptionKeyValueStoreSupplier.class, VanillaSubscriptionKeyValueStore::new);
        root.registerInterceptor(MapViewFactory.class, MapView::new);
        root.registerInterceptor(AssetFactory.class, VanillaAsset::new);
        root.registerInterceptor(KeyValueStoreFactory.class, VanillaKeyValueStore::new);
    }

    @NotNull
    @Override
    public <A> Asset acquireAsset(String name, Class<A> assetClass) throws AssetNotFoundException {
        return name.isEmpty() || name.equals("/") ? root : root.acquireChild(name, assetClass);
    }

    @Override
    public <I extends Interceptor> void registerInterceptor(Class<I> iClass, I interceptor) {
        root.registerInterceptor(iClass, interceptor);
    }

    @Nullable
    @Override
    public Asset getAsset(String name) {
        return name.isEmpty() || name.equals("/") ? root : root.getChild(name);
    }

    @Override
    public Asset add(String name, Assetted resource) {
        return root.add(name, resource);
    }

    @Override
    public void close() {
        root.close();
    }
}
