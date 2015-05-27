package net.openhft.chronicle.engine2.pubsub;

import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.Publisher;
import net.openhft.chronicle.engine2.api.map.MapView;

public class VanillaPublisher<E> implements Publisher<E> {
    private final String name;
    private final MapView<String, E, E> parentMap;

    public VanillaPublisher(FactoryContext<MapView<String, E, E>> context) {
        this.name = context.name();
        this.parentMap = context.item();
    }

    @Override
    public void publish(E event) {
        parentMap.put(name, event);
    }
}
