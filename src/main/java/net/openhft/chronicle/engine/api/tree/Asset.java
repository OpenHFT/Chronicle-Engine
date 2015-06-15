package net.openhft.chronicle.engine.api.tree;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.ThrowingAcceptor;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiPredicate;

/**
 * Created by peter on 22/05/15.
 */
public interface Asset extends Closeable {
    String name();

    Subscription subscription(boolean createIfAbsent) throws AssetNotFoundException;

    @NotNull
    default String fullName() {
        Asset parent = parent();
        return parent == null
                ? "/"
                : parent.parent() == null
                ? "/" + name()
                : parent.fullName() + "/" + name();
    }

    @Nullable
    Asset parent();

    @NotNull
    Asset acquireAsset(RequestContext context, String fullName) throws AssetNotFoundException;

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

    Asset getChild(String name);

    void removeChild(String name);

    boolean isReadOnly();

    @NotNull
    default <V> V acquireView(@NotNull RequestContext rc) throws AssetNotFoundException {
        return (V) acquireView(rc.viewType(), rc);
    }

    <V> V acquireView(Class<V> viewType, RequestContext rc) throws AssetNotFoundException;

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
