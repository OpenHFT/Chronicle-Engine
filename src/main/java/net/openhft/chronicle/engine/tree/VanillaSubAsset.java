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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingConsumer;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubAsset;
import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.SubscriptionCollection;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import net.openhft.chronicle.engine.pubsub.*;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSubAsset<E> implements SubAsset<E>, Closeable, TopicSubscriber<String, E> {
    @NotNull
    private final VanillaAsset parent;
    private final String name;
    @NotNull
    private final SimpleSubscription<E> subscription;
    @Nullable
    private final Reference<E> reference;

    public VanillaSubAsset(@NotNull VanillaAsset parent, String name, Class<E> type, Function<Object, E> valueReader) throws AssetNotFoundException {
        this.parent = parent;
        this.name = name;
        @Nullable TcpChannelHub tcpChannelHub = parent.findView(TcpChannelHub.class);
        if (tcpChannelHub == null) {
            @Nullable QueueView queueView = parent.getView(QueueView.class);
            if (queueView == null) {
                reference = new MapReference<>(name, type, this, parent.acquireView(MapView.class));
                subscription = new MapSimpleSubscription<>(reference, valueReader);
            } else {
                reference = new QueueReference<>(type, parent, queueView, name);
                subscription = new QueueSimpleSubscription<>(valueReader, parent, name);

            }
        } else {
            reference = new RemoteReference<>(tcpChannelHub, type, fullName());
            subscription = new RemoteSimpleSubscription<>(reference);
        }
    }

    @Override
    public String dumpRules() {
        return parent.dumpRules();
    }

    @NotNull
    @Override
    public SubscriptionCollection subscription(boolean createIfAbsent) {
        return subscription;
    }

    @NotNull
    @Override
    public <V> V getView(Class<V> viewType) {
        if (viewType == Reference.class || viewType == Publisher.class || viewType == Supplier.class)
            return (V) reference;
        if (viewType == SubscriptionCollection.class || viewType == MapSimpleSubscription.class
                || viewType == ObjectSubscription.class)
            return (V) subscription;
        throw new UnsupportedOperationException("Unable to classify view type " + viewType);
    }

    @Override
    public String name() {
        return name;
    }

    @NotNull
    @Override
    public <V> V acquireView(@NotNull Class<V> viewType, @NotNull RequestContext rc) throws AssetNotFoundException {
        if (viewType == Reference.class || viewType == Supplier.class) {
            return (V) reference;
        }
        if (viewType == Publisher.class) {
            if (reference == null)
                return acquireViewFor(viewType, rc);
            return (V) reference;
        }
        if (viewType == MapSimpleSubscription.class || viewType == ObjectSubscription.class) {
            return (V) subscription;
        }
        throw new UnsupportedOperationException("todo vClass: " + viewType + ", rc: " + rc);
    }

    @NotNull
    private <V> V acquireViewFor(@NotNull Class<V> viewType, @NotNull RequestContext rc) throws AssetNotFoundException {
        return parent.getView(viewType);
    }

    @Override
    public <V> V addView(Class<V> viewType, V view) {
        return view;
    }

    @Override
    public boolean isSubAsset() {
        return true;
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public <I> void registerView(Class<I> viewType, I view) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> viewType, String description, BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> viewType, String description, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <L> void addLeafRule(Class<L> viewType, String description, LeafViewFactory<L> factory) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void close() {
        subscription.close();
    }

    @Override
    public Asset parent() {
        return parent;
    }

    @NotNull
    @Override
    public Asset acquireAsset(String childName) throws AssetNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> boolean hasFactoryFor(Class<V> viewType) {
        return false;
    }

    @NotNull
    @Override
    public Asset getChild(String name) {
        return null;
    }

    @Override
    public void removeChild(String name) {
    }

    @Override
    public void onMessage(@NotNull String name, E e) {
        if (name.equals(this.name))
            subscription.notifyMessage(e);
    }

    @Override
    public <T extends Throwable> void forEachChild(ThrowingConsumer<Asset, T> consumer) throws T {
    }

    @Override
    public void getUsageStats(AssetTreeStats ats) {
    }
}
