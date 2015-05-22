package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Publisher<E> {
    void publish(E event);
}
