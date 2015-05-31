package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.collection.ValuesCollection;
import net.openhft.chronicle.engine2.api.map.*;
import net.openhft.chronicle.engine2.api.set.EntrySetView;
import net.openhft.chronicle.engine2.api.set.KeySetView;
import net.openhft.chronicle.engine2.map.*;
import net.openhft.chronicle.engine2.pubsub.VanillaReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSession implements Session {
    private static final String LAST = "{last}";
    final VanillaAsset root = new VanillaAsset(requestContext(""), null);

    public VanillaSession() {
        viewTypeLayersOn(MapView.class, LAST + " string key maps", SubscriptionKeyValueStore.class);

        root.addClassifier(Subscriber.class, LAST + " generic subscriber", rc ->
                        rc.elementType() == MapEvent.class ? (rc2, asset) -> asset.acquireFactory(MapEventSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : rc.elementType() == Map.Entry.class ? (rc2, asset) -> asset.acquireFactory(EntrySetSubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
                                : (rc2, asset) -> asset.acquireFactory(KeySubscriber.class).create(rc2, asset, () -> (Assetted) asset.acquireView(Subscription.class, rc2))
        );
        viewTypeLayersOn(MapEventSubscriber.class, LAST + " MapEvent subscriber", Subscription.class);
        viewTypeLayersOn(KeySubscriber.class, LAST + " keySet subscriber", Subscription.class);
        viewTypeLayersOn(EntrySetSubscriber.class, LAST + " entrySet subscriber", Subscription.class);
        viewTypeLayersOn(TopicSubscriber.class, LAST + " key,value topic subscriber", Subscription.class);

        viewTypeLayersOn(KeySetView.class, LAST + " keySet", MapView.class);
        viewTypeLayersOn(EntrySetView.class, LAST + " entrySet", MapView.class);
        viewTypeLayersOn(ValuesCollection.class, LAST + " values", MapView.class);

        viewTypeLayersOn(Publisher.class, LAST + "publisher", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(TopicPublisher.class, LAST + " topic publisher", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(StringStringKeyValueStore.class, LAST + " string -> string", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(StringMarshallableKeyValueStore.class, LAST + " string -> marshallable", SubscriptionKeyValueStore.class);
        viewTypeLayersOn(SubscriptionKeyValueStore.class, LAST + " string -> marshallable", KeyValueStore.class);

        root.addClassifier(Asset.class, LAST + " Asset", rc -> VanillaAsset::new);
        root.addClassifier(SubAsset.class, LAST + " SubAsset", rc -> VanillaSubAsset::new);

        root.registerFactory(MapView.class, VanillaMapView::new);
        root.registerFactory(SubscriptionKeyValueStore.class, VanillaSubscriptionKeyValueStore::new);
        root.registerFactory(EntrySetView.class, VanillaEntrySetView::new);
        root.registerFactory(KeyValueStore.class, VanillaKeyValueStore::new);
        root.registerFactory(TopicPublisher.class, VanillaTopicPublisher::new);

        root.registerFactory(Publisher.class, VanillaReference::new);
        root.registerFactory(Reference.class, VanillaReference::new);
    }

    public void viewTypeLayersOn(Class viewType, String description, Class underlyingType) {
        root.addClassifier(viewType, description, rc -> (rc2, asset) ->
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
