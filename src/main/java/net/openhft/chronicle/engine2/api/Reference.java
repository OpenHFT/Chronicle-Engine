package net.openhft.chronicle.engine2.api;

import java.util.function.Supplier;

public interface Reference<E> extends Publisher<E>, Supplier<E> {
    E get();

    default E getAndSet(E e) {
        E prev = get();
        set(e);
        return prev;
    }

    void set(E e);

    void remove();

    default E getAndRemove() {
        E prev = get();
        remove();
        return prev;
    }

    default void publish(E e) {
        set(e);
    }
}
