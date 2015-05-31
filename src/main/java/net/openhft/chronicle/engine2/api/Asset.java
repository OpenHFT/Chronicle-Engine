package net.openhft.chronicle.engine2.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Created by peter on 22/05/15.
 */
public interface Asset extends Permissoned {
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
    Stream<Asset> children();

    Asset add(String name, Assetted resource);

    @NotNull
    <A> Asset acquireChild(Class<A> assetClass, RequestContext context, String name) throws AssetNotFoundException;

    @Nullable
    default Asset getAsset(String name) {
        if (name.isEmpty()) return this;
        int pos = name.indexOf("/");
        if (pos >= 0) {
            String name1 = name.substring(0, pos);
            String name2 = name.substring(pos + 1);
            Asset asset = getChild(name1);
            if (asset == null) {
                return null;

            } else {
                return asset.getAsset(name2);
            }
        }
        return getChild(name);
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
}
