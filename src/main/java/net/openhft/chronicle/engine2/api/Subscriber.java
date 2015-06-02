package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Subscriber<E> extends ISubscriber {
    void onMessage(E e) throws InvalidSubscriberException;
}
