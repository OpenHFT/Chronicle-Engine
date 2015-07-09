package net.openhft.chronicle.engine.pubsub;

import net.openhft.chronicle.engine.api.pubsub.Subscription;

/**
 * Created by peter.lawrey on 09/07/2015.
 */
public interface SimpleSubscription<E> extends Subscription<E> {

    void notifyMessage(Object e);

}
