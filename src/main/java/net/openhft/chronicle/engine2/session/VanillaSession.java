package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.*;
import net.openhft.chronicle.engine2.map.*;
import net.openhft.chronicle.engine2.pubsub.VanillaReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.engine2.api.FactoryContext.factoryContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSession implements Session {
    final VanillaAsset root = new VanillaAsset(factoryContext(null).name(""));

    public VanillaSession() {
        root.registerFactory(MapView.class, VanillaMapView::new);
        root.registerFactory(StringMarshallableKeyValueStore.class, VanillaStringMarshallableKeyValueStore::new);
        root.registerFactory(SubscriptionKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);
        root.registerFactory(EntrySetView.class, VanillaEntrySetView::new);
        root.registerFactory(KeyValueStore.class, VanillaKeyValueStore::new);

        root.registerFactory(Asset.class, VanillaAsset::new);
        root.registerFactory(SubAsset.class, VanillaSubAsset::new);
        root.registerFactory(TopicPublisher.class, VanillaTopicPublisher::new);
        root.registerFactory(Publisher.class, VanillaReference::new);
        root.registerFactory(Reference.class, VanillaReference::new);
        root.registerFactory(Subscription.class, f -> (Subscription) f.parent.acquireView(SubscriptionKeyValueStore.class, f.queryString()));
    }

    @NotNull
    @Override
    public <A> Asset acquireAsset(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException {
        return name.isEmpty() || name.equals("/") ? root : root.acquireChild(name, assetClass, class1, class2);
    }

    @Override
    public <I> void registerView(Class<I> iClass, I interceptor) {
        root.registerView(iClass, interceptor);
    }

    @Nullable
    @Override
    public Asset getAsset(String name) {
        return name.isEmpty() || name.equals("/") ? root : root.getAsset(name);
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
