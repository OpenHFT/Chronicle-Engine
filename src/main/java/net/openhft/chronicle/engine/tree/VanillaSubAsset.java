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

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.map.SubAsset;
import net.openhft.chronicle.engine.api.map.ValueReader;
import net.openhft.chronicle.engine.api.pubsub.*;
import net.openhft.chronicle.engine.api.tree.*;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.pubsub.SimpleSubscription;
import net.openhft.chronicle.engine.pubsub.VanillaReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiPredicate;
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
    private Reference<E> reference;

    VanillaSubAsset(@NotNull VanillaAsset parent, String name, Class<E> type, ValueReader valueReader) throws AssetNotFoundException {
        this.parent = parent;
        this.name = name;
        reference = new VanillaReference<E>(name, type, parent.getView(MapView.class));
        subscription = new SimpleSubscription<>(reference, valueReader == null ? ValueReader.PASS : valueReader);
    }

    @NotNull
    @Override
    public Subscription subscription(boolean createIfAbsent) {
        return subscription;
    }

    @NotNull
    @Override
    public <V> V getView(Class<V> vClass) {
        if (vClass == Reference.class || vClass == Publisher.class || vClass == Supplier.class)
            return (V) reference;
        if (vClass == Subscription.class || vClass == SimpleSubscription.class)
            return (V) subscription;
        throw new UnsupportedOperationException("todo");
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
        if (viewType == SimpleSubscription.class || viewType == ObjectKVSSubscription.class) {
            return (V) subscription;
        }
        throw new UnsupportedOperationException("todo vClass: " + viewType + ", rc: " + rc);
    }

    @NotNull
    private <V> V acquireViewFor(@NotNull Class<V> viewType, @NotNull RequestContext rc) throws AssetNotFoundException {
        return parent.getView(viewType);
    }

    @Override
    public <V> V addView(Class<V> viewType, V v) {
        return v;
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
    public <I> void registerView(Class<I> iClass, I interceptor) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> iClass, String description, BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <W, U> void addWrappingRule(Class<W> iClass, String description, WrappingViewFactory<W, U> factory, Class<U> underlyingType) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <L> void addLeafRule(Class<L> iClass, String description, LeafViewFactory<L> factory) {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public <I, U> I createWrappingView(Class viewType, RequestContext rc, Asset asset, U underling) throws AssetNotFoundException {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    @Override
    public <I> I createLeafView(Class viewType, RequestContext rc, Asset asset) throws AssetNotFoundException {
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
    public boolean keyedView() {
        return false;
    }

    @Override
    public void forEachChild(ThrowingAcceptor<Asset, InvalidSubscriberException> child) throws InvalidSubscriberException {
    }
}
