package net.openhft.chronicle.engine.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

/**
 * Created by peter on 22/05/15.
 */
public interface Asset {
    String name();

    Subscription subscription(boolean createIfAbsent);

    default String fullName() {
        return parent() == null
                ? "/"
                : parent().parent() == null
                ? name()
                : parent().fullName() + "/" + name();
    }

    @Nullable
    Asset parent();

    @NotNull
    Asset acquireAsset(RequestContext context, String fullName) throws AssetNotFoundException;

    @Nullable
    default Asset getAsset(String fullName) {
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

    default <V> V acquireView(RequestContext rc) {
        return (V) acquireView(rc.viewType(), rc);
    }

    <V> V acquireView(Class<V> viewType, RequestContext rc);

    <V> V getView(Class<V> vClass);

    <I> void registerView(Class<I> iClass, I interceptor);

    <I> ViewFactory<I> getFactory(Class<I> iClass);

    <I> ViewFactory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException;

    <I> void registerFactory(Class<I> iClass, ViewFactory<I> factory);

    <V> void addClassifier(Class<V> assetType, String name, Function<RequestContext, ViewLayer> viewBuilderFactory);

    ViewLayer classify(Class viewType, RequestContext rc) throws AssetNotFoundException;


    boolean isSubAsset();

    default void viewTypeLayersOn(Class viewType, String description, Class underlyingType) {
        addClassifier(viewType, description, rc -> (rc2, asset) ->
                (View) asset.acquireFactory(viewType).create(rc2, asset, () -> (Assetted) asset.acquireView(underlyingType, rc2)));
    }

    default Asset root() {
        return parent() == null ? this : parent().root();
    }
}
