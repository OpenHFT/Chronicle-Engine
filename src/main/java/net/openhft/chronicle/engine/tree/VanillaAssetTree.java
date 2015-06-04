package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.map.*;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.pubsub.VanillaReference;
import net.openhft.chronicle.engine.session.VanillaSessionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAssetTree implements AssetTree {
    private static final String LAST = "{last}";
    final VanillaAsset root = new VanillaAsset(null, "");

    public VanillaAssetTree() {

        root.viewTypeLayersOn(TopicPublisher.class, LAST + " topic publisher", MapView.class);
        root.registerFactory(TopicPublisher.class, VanillaTopicPublisher::new);

        root.viewTypeLayersOn(Reference.class, LAST + "reference", MapView.class);
        root.registerFactory(Reference.class, VanillaReference::new);

        root.viewTypeLayersOn(Publisher.class, LAST + "publisher", MapView.class);
        root.registerFactory(Publisher.class, VanillaReference::new);

        root.viewTypeLayersOn(EntrySetView.class, LAST + " entrySet", MapView.class);
        root.registerFactory(EntrySetView.class, VanillaEntrySetView::new);

        root.viewTypeLayersOn(ValuesCollection.class, LAST + " values", MapView.class);

        root.viewTypeLayersOn(MapEventSubscriber.class, LAST + " MapEvent subscriber", Subscription.class);
// todo CE-54      root.registerFactory(MapEventSubscriber.class, VanillaMapEventSubscriber::new);

        root.viewTypeLayersOn(KeySubscriber.class, LAST + " keySet subscriber", Subscription.class);
// todo CE-54      root.registerFactory(KeySubscriber.class, VanillaKeySubscriber::new);

        root.viewTypeLayersOn(EntrySetSubscriber.class, LAST + " entrySet subscriber", Subscription.class);
// todo  CE-54     root.registerFactory(EntrySetView.class, VanillaEntrySetSubscriber::new);

        root.viewTypeLayersOn(KeySetView.class, LAST + " keySet", MapView.class);
// todo  CE-54     root.registerFactory(KeySetView.class, VanillaKeySetView::new);

        root.viewTypeLayersOn(TopicSubscriber.class, LAST + " key,value topic subscriber", Subscription.class);
// todo   CE-54    root.registerFactory(TopicSubscriber.class, VanillaTopicSubscriber::new);

        root.addClassifier(Subscriber.class, LAST + " generic subscriber", rc ->
                        rc.elementType() == MapEvent.class ? (rc2, asset) -> asset.acquireFactory(MapEventSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : rc.elementType() == Map.Entry.class ? (rc2, asset) -> asset.acquireFactory(EntrySetSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : (rc2, asset) -> asset.acquireFactory(KeySubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
        );

        root.viewTypeLayersOn(MapView.class, LAST + " string key maps", AuthenticatedKeyValueStore.class);
        root.registerFactory(MapView.class, VanillaMapView::new);

        root.viewTypeLayersOn(AuthenticatedKeyValueStore.class, LAST + " string -> marshallable", KeyValueStore.class);
        root.registerFactory(AuthenticatedKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);

        root.viewTypeLayersOn(SubscriptionKeyValueStore.class, LAST + " sub -> foundation", KeyValueStore.class);
        root.registerFactory(SubscriptionKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);

        root.registerFactory(KeyValueStore.class, VanillaKeyValueStore::new);

        root.addView(SessionProvider.class, new VanillaSessionProvider(root));
    }

    @NotNull
    @Override
    public <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws AssetNotFoundException {
        String name = context.fullName();
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
