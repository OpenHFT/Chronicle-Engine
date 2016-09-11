/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.query.Filter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by peter on 11/06/15.
 */
public class VanillaTopologySubscription implements TopologySubscription {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaTopologySubscription.class);

    private final Asset asset;
    private final Set<Subscriber<TopologicalEvent>> subscribers = new CopyOnWriteArraySet<>();

    public VanillaTopologySubscription(RequestContext requestContext, Asset asset) {
        this.asset = asset;
    }

    void bootstrapTree(@NotNull Asset asset, @NotNull Subscriber<TopologicalEvent> subscriber) throws InvalidSubscriberException {
        asset.forEachChild(c -> {
            subscriber.onMessage(ExistingAssetEvent.of(asset.fullName(), c.name()));
            bootstrapTree(c, subscriber);
        });
    }

    @Override
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber<TopologicalEvent> subscriber, @NotNull Filter<TopologicalEvent> filter) {
        try {
            if (rc.bootstrap() != Boolean.FALSE) {
                // root node.
                Asset parent = asset.parent();
                String assetName = parent == null ? null : parent.fullName();
                subscriber.onMessage(ExistingAssetEvent.of(assetName, asset.name()));
                bootstrapTree(asset, subscriber);
            }
            subscribers.add(subscriber);
        } catch (InvalidSubscriberException e) {
            Jvm.debug().on(getClass(), "discarding " + subscriber, e);
        }
    }

    @Override
    public void unregisterSubscriber(@NotNull Subscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    public int keySubscriberCount() {
        return 0;
    }

    @Override
    public int entrySubscriberCount() {
        return subscriberCount();
    }

    @Override
    public int topicSubscriberCount() {
        return 0;
    }

    @Override
    public int subscriberCount() {
        return subscribers.size();
    }

    @Override
    public void notifyEvent(TopologicalEvent event) {
        for (Subscriber<TopologicalEvent> sub : subscribers) {
            try {
                sub.onMessage(event);
            } catch (InvalidSubscriberException expected) {
                subscribers.remove(sub);
            }
        }
        Asset parent = asset.parent();
        if (parent != null) {
            TopologySubscription topologySubscription = parent.findView(TopologySubscription.class);
            if (topologySubscription != null)
                topologySubscription.notifyEvent(event);
        }
    }

    @Override
    public void close() {
        for (Subscriber<TopologicalEvent> subscriber : subscribers) {
            try {
                subscriber.onEndOfSubscription();
            } catch (Exception e) {
                Jvm.debug().on(getClass(), e);
            }
        }
    }
}
