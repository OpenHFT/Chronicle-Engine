/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by peter on 11/06/15.
 */
public class VanillaTopologySubscription implements TopologySubscription {
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
    public void registerSubscriber(@NotNull RequestContext rc, @NotNull Subscriber<TopologicalEvent> subscriber) {
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
            // ignored
        }
    }

    @Override
    public void unregisterSubscriber(Subscriber<TopologicalEvent> subscriber) {
        subscribers.remove(subscriber);
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
                e.printStackTrace();
            }
        }
    }
}
