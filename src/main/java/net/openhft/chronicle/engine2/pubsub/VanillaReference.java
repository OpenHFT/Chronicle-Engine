package net.openhft.chronicle.engine2.pubsub;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.MapView;

import java.util.function.Supplier;

import static net.openhft.chronicle.engine2.api.RequestContext.requestContext;

public class VanillaReference<E> implements Reference<E> {
    private final String name;
    private final Class<E> eClass;
    private final MapView<String, E, E> underlyingMap;

    public VanillaReference(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this.name = context.name();
        this.eClass = context.type();
        this.underlyingMap = (MapView<String, E, E>) assetted.get();
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
    public void registerSubscriber(Subscriber<E> subscriber) {
        underlyingMap.asset().getChild(name)
                .subscription(true)
                .registerSubscriber(requestContext().bootstrap(true).type(eClass), subscriber);
    }
}
