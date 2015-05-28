package net.openhft.chronicle.engine2.api;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.stream.Stream;

/**
 * Created by peter on 22/05/15.
 */
public interface Asset extends Permissoned, Subscription {
    String name();

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
    <A> Asset acquireChild(String name, Class<A> assetClass, Class class1, Class class2) throws AssetNotFoundException;

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

    default <V> V acquireView(Class<V> vClass) {
        return acquireView(vClass, "");
    }

    default <V> V acquireView(Class<V> vClass, String queryString) {
        return acquireView(vClass, null, "");
    }

    default <V> V acquireView(Class<V> vClass, Class class1, String queryString) {
        return acquireView(vClass, class1, null, queryString);
    }

    <I> I acquireView(Class<I> vClass, Class class1, Class class2, String queryString);

    <V> V getView(Class<V> vClass);

    <I> void registerView(Class<I> iClass, I interceptor);

    <I> Factory<I> getFactory(Class<I> iClass);

    <I> Factory<I> acquireFactory(Class<I> iClass) throws AssetNotFoundException;

    <I> void registerFactory(Class<I> iClass, Factory<I> factory);

    Object item();
}
