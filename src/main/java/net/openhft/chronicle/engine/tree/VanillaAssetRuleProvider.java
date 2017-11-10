/*
 * Copyright 2014-2017 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.collection.ValuesCollection;
import net.openhft.chronicle.engine.api.column.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Replication;
import net.openhft.chronicle.engine.api.pubsub.TopicPublisher;
import net.openhft.chronicle.engine.api.query.IndexQueueView;
import net.openhft.chronicle.engine.api.query.VanillaIndexQueueView;
import net.openhft.chronicle.engine.api.set.EntrySetView;
import net.openhft.chronicle.engine.api.set.KeySetView;
import net.openhft.chronicle.engine.collection.VanillaValuesCollection;
import net.openhft.chronicle.engine.map.*;
import net.openhft.chronicle.engine.map.remote.RemoteIndexQueueView;
import net.openhft.chronicle.engine.map.remote.RemoteKVSSubscription;
import net.openhft.chronicle.engine.map.remote.RemoteKeyValueStore;
import net.openhft.chronicle.engine.map.remote.RemoteMapView;
import net.openhft.chronicle.engine.pubsub.*;
import net.openhft.chronicle.engine.queue.QueueWrappingColumnView;
import net.openhft.chronicle.engine.set.RemoteKeySetView;
import net.openhft.chronicle.engine.set.VanillaKeySetView;
import net.openhft.chronicle.engine.tree.*;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.engine.tree.VanillaAsset.LAST;

public class VanillaAssetRuleProvider implements AssetRuleProvider {

    public void configMapCommon(@NotNull VanillaAsset asset) {
        asset.addWrappingRule(ValuesCollection.class, LAST + " values", VanillaValuesCollection::new, MapView.class);
        asset.addView(SubAssetFactory.class, new VanillaSubAssetFactory());
    }

    public void configMapServer(@NotNull VanillaAsset asset) {
        configMapCommon(asset);
        asset.addWrappingRule(EntrySetView.class, LAST + " VanillaEntrySetView", VanillaEntrySetView::new, MapView.class);
        asset.addWrappingRule(KeySetView.class, LAST + " VanillaKeySetView", VanillaKeySetView::new, MapView.class);
        asset.addWrappingRule(Reference.class, LAST + "reference", MapReference::new, MapView.class);
        asset.addWrappingRule(Replication.class, LAST + "replication", VanillaReplication::new, MapView.class);
        asset.addWrappingRule(Publisher.class, LAST + " MapReference", MapReference::new, MapView.class);
        asset.addWrappingRule(TopicPublisher.class, LAST + " MapTopicPublisher", MapTopicPublisher::new, MapView.class);
        asset.addWrappingRule(MapView.class, LAST + " VanillaMapView", VanillaMapView::new, ObjectKeyValueStore.class);
        asset.addWrappingRule(MapColumnView.class, LAST + "Map ColumnView", MapWrappingColumnView::new,
                MapView.class);

        // storage options
        asset.addLeafRule(ObjectSubscription.class, LAST + " vanilla", MapKVSSubscription::new);
        asset.addLeafRule(RawKVSSubscription.class, LAST + " vanilla", MapKVSSubscription::new);
        asset.addWrappingRule(ObjectKeyValueStore.class, LAST + " VanillaSubscriptionKeyValueStore",
                VanillaSubscriptionKeyValueStore::new, AuthenticatedKeyValueStore.class);

        asset.addLeafRule(AuthenticatedKeyValueStore.class, LAST + " VanillaKeyValueStore", VanillaKeyValueStore::new);
        asset.addLeafRule(SubscriptionKeyValueStore.class, LAST + " VanillaKeyValueStore", VanillaKeyValueStore::new);
        asset.addLeafRule(KeyValueStore.class, LAST + " VanillaKeyValueStore", VanillaKeyValueStore::new);
        asset.addLeafRule(VaadinChart.class, LAST + " VaadinChart", VanillaVaadinChart::new);
    }

    public void configMapRemote(@NotNull VanillaAsset asset) {
        configMapCommon(asset);
        asset.addWrappingRule(SimpleSubscription.class, LAST + "subscriber", RemoteSimpleSubscription::new, Reference.class);

        asset.addWrappingRule(EntrySetView.class, LAST + " RemoteEntrySetView", RemoteEntrySetView::new, MapView.class);
        asset.addWrappingRule(KeySetView.class, LAST + " RemoteKeySetView", RemoteKeySetView::new, MapView.class);
        asset.addLeafRule(Publisher.class, LAST + " RemotePublisher", RemotePublisher::new);
        asset.addLeafRule(Reference.class, LAST + "reference", RemoteReference::new);
        asset.addLeafRule(TopicPublisher.class, LAST + " RemoteTopicPublisher", RemoteTopicPublisher::new);

        asset.addWrappingRule(MapView.class, LAST + " RemoteMapView", RemoteMapView::new, ObjectKeyValueStore.class);

        asset.addLeafRule(ObjectKeyValueStore.class, LAST + " RemoteKeyValueStore",
                RemoteKeyValueStore::new);
        asset.addLeafRule(ObjectSubscription.class, LAST + " Remote", RemoteKVSSubscription::new);
        asset.addLeafRule(VaadinChart.class, LAST + " VanillaKeyValueStore", RemoteVaadinChart::new);
    }


    public void configQueueCommon(@NotNull VanillaAsset asset) {
        asset.addWrappingRule(Reference.class, LAST + "QueueReference",
                QueueReference::new, QueueView.class);
    }

    public void configQueueServer(@NotNull VanillaAsset asset) {
        configQueueCommon(asset);
        asset.addWrappingRule(Publisher.class, LAST + " QueueReference",
                QueueReference::new, QueueView.class);

        asset.addWrappingRule(TopicPublisher.class, LAST + " QueueTopicPublisher",
                QueueTopicPublisher::new, QueueView.class);

        asset.addLeafRule(ObjectSubscription.class, LAST + " QueueObjectSubscription",
                QueueObjectSubscription::new);

        asset.addWrappingRule(IndexQueueView.class, LAST + " VanillaIndexQueueView",
                VanillaIndexQueueView::new, QueueView.class);

        asset.addLeafRule(QueueView.class, LAST + " ChronicleQueueView", ChronicleQueueView::create);

        asset.addWrappingRule(QueueColumnView.class, LAST + "Queue ColumnView",
                QueueWrappingColumnView::new, QueueView.class);
    }

    /**
     * the wrapping rules for the connector of the TCP/IP connection
     */
    public void configQueueRemote(@NotNull VanillaAsset asset) {
        configQueueCommon(asset);
        asset.addLeafRule(QueueView.class, LAST + " RemoteQueueView", RemoteQueueView::new);

        asset.addLeafRule(IndexQueueView.class, LAST + " RemoteIndexQueueView",
                RemoteIndexQueueView::new);
    }

    public void configColumnViewRemote(@NotNull VanillaAsset asset) {
        asset.addLeafRule(ColumnView.class, LAST + " Remote", RemoteColumnView::new);
        asset.addLeafRule(MapColumnView.class, LAST + " Remote", RemoteColumnView::new);
        asset.addLeafRule(QueueColumnView.class, LAST + " Remote", RemoteColumnView::new);
    }
}
