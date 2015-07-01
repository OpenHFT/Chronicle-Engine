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

package net.openhft.chronicle.engine.api.tree;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiPredicate;

/**
 * An Asset is a point on an AssetTree.  An Asset can not only have any number of Children it has multiple views depending on how you want to access the data.
 * <p></p>
 * A single asset can have a MapView, EntrySetView, KeySetView, ValuesCollection, Subscription, TopicPublisher. A Map may be viewed in terms of the objects it holds e.g. String or Marshallable or the raw data i.e. as a BytesStore.
 */
public interface Asset extends Closeable {
    /**
     * @return the name of this asset (not including the group)
     */
    String name();

    /**
     * @return the parent of this asset or null if it is the root.
     */
    @Nullable
    Asset parent();

    /**
     * @return the full name of this asset including it's parent and theirs etc.
     */
    @NotNull
    default String fullName() {
        Asset parent = parent();
        return parent == null
                ? "/"
                : parent.parent() == null
                ? "/" + name()
                : parent.fullName() + "/" + name();
    }

    /**
     * Obtain the default subscription view. If there is more than one this will be the object subscription view.
     *
     * @param createIfAbsent create one if it doesn't exist.
     * @return the Subscription
     * @throws AssetNotFoundException if the Subscription doesn't exist and the tree is not able to create the Subscription.
     */
    @NotNull
    Subscription subscription(boolean createIfAbsent) throws AssetNotFoundException;

    /**
     * Navigate down the tree to find an asset.
     *
     * @param fullName with names separated by /
     * @return the Asset found or null
     */
    @Nullable
    default Asset getAsset(@NotNull String fullName) {
        if (fullName.isEmpty()) return this;
        int pos = fullName.indexOf("/");
        if (pos >= 0) {
            String name1 = fullName.substring(0, pos);
            String name2 = fullName.substring(pos + 1);
            Asset asset = getChild(name1);
            if (asset == null) {
                return null;

            } else {
                return asset.getAsset(name2);
            }
        }
        return getChild(fullName);
    }

    /**
     * Get or create an asset under this one.
     *
     * @param childName name of the child asset.
     * @return
     * @throws AssetNotFoundException
     */
    @NotNull
    Asset acquireAsset(String childName) throws AssetNotFoundException;

    /**
     * Search a tree to find the first Asset with the name given.
     *
     * @param name partial name of asset to find.
     * @return the Asset found or null.
     */
    @Nullable
    default Asset findAsset(@NotNull String name) {
        Asset asset = getAsset(name);
        Asset parent = parent();
        if (asset == null && parent != null)
            asset = parent.findAsset(name);
        return asset;
    }

    /**
     * Search for a view up the tree.
     *
     * @param viewType the class pf the view
     * @return the View found or null.
     */
    @Nullable
    default <V> V findView(@NotNull Class<V> viewType) {
        V v = getView(viewType);
        Asset parent = parent();
        if (v == null && parent != null)
            v = parent.findView(viewType);
        return v;
    }

    /**
     * Search up the tree for a view of viewType.  If one doesn't exist find a factory and add it to the asset the factory is associated with.  This view can then be shared for the Assets under the Asset where the factory is defined.
     *
     * @param viewType to obtain.
     * @return the view it could be created
     */
    @Nullable
    default <V> V findOrCreateView(@NotNull Class<V> viewType) throws AssetNotFoundException {
        V v = getView(viewType);
        if (v == null) {
            if (hasFactoryFor(viewType))
                return acquireView(viewType, RequestContext.requestContext());
            Asset parent = parent();
            if (parent != null)
                v = parent.findOrCreateView(viewType);
        }
        return v;
    }

    <V> boolean hasFactoryFor(Class<V> viewType);

    Asset getChild(String name);

    void removeChild(String name);

    @NotNull
    default <V> V acquireView(@NotNull RequestContext rc) throws AssetNotFoundException {
        return (V) acquireView(rc.viewType(), rc);
    }

    @NotNull
    <V> V acquireView(Class<V> viewType, RequestContext rc) throws AssetNotFoundException;

    @Nullable
    <V> V getView(Class<V> vClass);

    <I> void registerView(Class<I> iClass, I interceptor);

    <W, U> void addWrappingRule(Class<W> iClass, String description, BiPredicate<RequestContext, Asset> predicate, WrappingViewFactory<W, U> factory, Class<U> underlyingType);

    <W, U> void addWrappingRule(Class<W> iClass, String description, WrappingViewFactory<W, U> factory, Class<U> underlyingType);

    <L> void addLeafRule(Class<L> iClass, String description, LeafViewFactory<L> factory);

    <I, U> I createWrappingView(Class viewType, RequestContext rc, Asset asset, U underling) throws AssetNotFoundException;

    <I> I createLeafView(Class viewType, RequestContext rc, Asset asset) throws AssetNotFoundException;

    <V> V addView(Class<V> viewType, V v);

    boolean isSubAsset();

    @NotNull
    default Asset root() {
        return parent() == null ? this : parent().root();
    }

    boolean hasChildren();

    void forEachChild(ThrowingAcceptor<Asset, InvalidSubscriberException> child) throws InvalidSubscriberException;
}
