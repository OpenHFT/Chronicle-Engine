package net.openhft.chronicle.engine.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Publisher<E> {
    void publish(E event);

    void registerSubscriber(Subscriber<E> subscriber);
}
