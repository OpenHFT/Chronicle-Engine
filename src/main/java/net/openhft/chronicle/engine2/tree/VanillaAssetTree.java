package net.openhft.chronicle.engine2.tree;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.collection.ValuesCollection;
import net.openhft.chronicle.engine2.api.map.*;
import net.openhft.chronicle.engine2.api.set.EntrySetView;
import net.openhft.chronicle.engine2.api.set.KeySetView;
import net.openhft.chronicle.engine2.map.*;
import net.openhft.chronicle.engine2.pubsub.VanillaReference;
import net.openhft.chronicle.engine2.session.VanillaSessionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaAssetTree implements AssetTree {
    private static final String LAST = "{last}";
    final VanillaAsset root = new VanillaAsset(requestContext(""), null);

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
// todo       root.registerFactory(MapEventSubscriber.class, VanillaMapEventSubscriber::new);

        root.viewTypeLayersOn(KeySubscriber.class, LAST + " keySet subscriber", Subscription.class);
// todo       root.registerFactory(KeySubscriber.class, VanillaKeySubscriber::new);

        root.viewTypeLayersOn(EntrySetSubscriber.class, LAST + " entrySet subscriber", Subscription.class);
// todo       root.registerFactory(EntrySetView.class, VanillaEntrySetSubscriber::new);

        root.viewTypeLayersOn(KeySetView.class, LAST + " keySet", MapView.class);
// todo       root.registerFactory(KeySetView.class, VanillaKeySetView::new);

        root.viewTypeLayersOn(TopicSubscriber.class, LAST + " key,value topic subscriber", Subscription.class);
// todo       root.registerFactory(TopicSubscriber.class, VanillaTopicSubscriber::new);

        root.addClassifier(Subscriber.class, LAST + " generic subscriber", rc ->
                        rc.elementType() == MapEvent.class ? (rc2, asset) -> asset.acquireFactory(MapEventSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : rc.elementType() == Map.Entry.class ? (rc2, asset) -> asset.acquireFactory(EntrySetSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : (rc2, asset) -> asset.acquireFactory(KeySubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
        );

        root.viewTypeLayersOn(MapView.class, LAST + " string key maps", SubscriptionKeyValueStore.class);
        root.registerFactory(MapView.class, VanillaMapView::new);

        root.viewTypeLayersOn(SubscriptionKeyValueStore.class, LAST + " string -> marshallable", KeyValueStore.class);
        root.registerFactory(SubscriptionKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);

        root.addClassifier(Asset.class, LAST + " Asset", rc -> VanillaAsset::new);
        root.addClassifier(SubAsset.class, LAST + " SubAsset", rc -> VanillaSubAsset::new);

        root.registerFactory(KeyValueStore.class, VanillaKeyValueStore::new);

        root.addView(SessionProvider.class, new VanillaSessionProvider(root));
    }

    @NotNull
    @Override
    public <A> Asset acquireAsset(Class<A> assetClass, RequestContext context) throws AssetNotFoundException {
        String name = context.fullName();
        return name.isEmpty() || name.equals("/") ? root : root.acquireChild(assetClass, context, name);
    }

    @Nullable
    @Override
    public Asset getAsset(String fullName) {
        return fullName.isEmpty() || fullName.equals("/") ? root : root.getAsset(fullName);
    }

    @Override
    public Asset add(String fullName, Assetted resource) {
        return root.add(fullName, resource);
    }

    @Override
    public void close() {
        root.close();
    }

    public void viewTypeLayersOn(Class viewType, String description, Class underlyingType) {
        root.viewTypeLayersOn(viewType, description, underlyingType);
    }
}
