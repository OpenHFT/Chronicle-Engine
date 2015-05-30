package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.collection.ValuesCollection;
import net.openhft.chronicle.engine2.api.map.*;
import net.openhft.chronicle.engine2.api.set.EntrySetView;
import net.openhft.chronicle.engine2.api.set.KeySetView;
import net.openhft.chronicle.engine2.map.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSession implements Session {
    final VanillaAsset root = new VanillaAsset(requestContext(""), null, null);

    public VanillaSession() {
        viewTypeLayersOn(MapView.class, "string key maps", SubscriptionKeyValueStore.class);

        root.prependClassifier(Subscriber.class, "generic subscriber", rc ->
                        rc.elementType() == MapEvent.class ? (rc2, asset) -> asset.acquireFactory(MapEventSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : rc.elementType() == Map.Entry.class ? (rc2, asset) -> asset.acquireFactory(EntrySetSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : (rc2, asset) -> asset.acquireFactory(KeySubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
        );
        viewTypeLayersOn(MapEventSubscriber.class, "MapEvent subscriber", Subscription.class);
        viewTypeLayersOn(KeySubscriber.class, "keySet subscriber", Subscription.class);
        viewTypeLayersOn(EntrySetSubscriber.class, "entrySet subscriber", Subscription.class);
        viewTypeLayersOn(TopicSubscriber.class, "key,value topic subscriber", Subscription.class);

        viewTypeLayersOn(KeySetView.class, "keySet", MapView.class);
        viewTypeLayersOn(EntrySetView.class, "entrySet", MapView.class);
        viewTypeLayersOn(ValuesCollection.class, "values", MapView.class);

        viewTypeLayersOn(Publisher.class, "publisher", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(TopicPublisher.class, "topic publisher", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(StringStringKeyValueStore.class, "string -> string", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(StringMarshallableKeyValueStore.class, "string -> marshallable", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(SubscriptionKeyValueStore.class, "string -> marshallable", KeyValueStore.class);

        root.registerFactory(MapView.class, VanillaMapView::new);
        root.registerFactory(SubscriptionKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);
        root.registerFactory(EntrySetView.class, VanillaEntrySetView::new);
        root.registerFactory(KeyValueStore.class, VanillaKeyValueStore::new);

        root.registerFactory(Asset.class, VanillaAsset::new);
        root.registerFactory(SubAsset.class, VanillaSubAsset::new);
        root.registerFactory(TopicPublisher.class, VanillaTopicPublisher::new);

//        root.registerFactory(Publisher.class, VanillaReference::new);
//        root.registerFactory(Reference.class, VanillaReference::new);
    }

    public void viewTypeLayersOn(Class viewType, String description, Class underlyingType) {
        root.prependClassifier(viewType, description, rc -> (rc2, asset) ->
                (View) asset.acquireFactory(viewType).create(rc2, asset, () -> (Assetted) asset.acquireView(underlyingType, rc2)));
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
}
