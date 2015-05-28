package net.openhft.chronicle.engine2.pubsub;

import net.openhft.chronicle.engine2.api.Reference;
import net.openhft.chronicle.engine2.api.RequestContext;
import net.openhft.chronicle.engine2.api.Subscriber;
import net.openhft.chronicle.engine2.api.map.MapView;

public class VanillaReference<E> implements Reference<E> {
    private final String name;
    private final MapView<String, E, E> parentMap;
    private final Class<E> eClass;

    public VanillaReference(RequestContext<MapView<String, E, E>> context) {
        this.name = context.name();
        this.eClass = context.type();
        this.parentMap = context.item();
    }

    @Override
    public void set(E event) {
        parentMap.put(name, event);
    }

    @Override
    public E get() {
        return parentMap.get(name);
    }

    @Override
    public void remove() {
        parentMap.remove(name);
    }

    @Override
    public void registerSubscriber(Subscriber<E> subscriber) {
        parentMap.asset().getChild(name).registerSubscriber(eClass, subscriber, "bootstrap=true");
    }
}
