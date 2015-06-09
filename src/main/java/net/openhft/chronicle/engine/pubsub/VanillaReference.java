package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.MapView;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.engine.api.RequestContext.requestContext;

public class VanillaReference<E> implements Reference<E> {
    private final String name;
    private final Class<E> eClass;
    private final MapView<String, E, E> underlyingMap;

    public VanillaReference(@NotNull RequestContext context, Asset asset, @NotNull ThrowingSupplier<Assetted, AssetNotFoundException> assettedSupplier) throws AssetNotFoundException {
        this(context.name(), context.type(), (MapView<String, E, E>) assettedSupplier.get());
    }

    public VanillaReference(String name, Class type, MapView<String, E, E> mapView) {
        this.name = name;
        this.eClass = type;
        this.underlyingMap = mapView;
    }

    @Override
    public void set(E event) {
        underlyingMap.put(name, event);
    }

    @Override
    public E get() {
        return underlyingMap.get(name);
    }

    @Override
    public void remove() {
        underlyingMap.remove(name);
    }

    @Override
    public void registerSubscriber(Subscriber<E> subscriber) throws AssetNotFoundException {
        underlyingMap.asset().getChild(name)
                .subscription(true)
                .registerSubscriber(requestContext().bootstrap(true).type(eClass), subscriber);
    }
}
