package net.openhft.chronicle.engine2.api;

public interface Reference<E> extends Publisher<E> {
    E get();

    void set(E e);

    void remove();

    default void publish(E e) {
        set(e);
    }
}
