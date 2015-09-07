package net.openhft.chronicle.engine.api.query;

/**
 * Created by peter.lawrey on 07/09/2015.
 */
public interface Subscription {
    void cancel();

    boolean isComplete();
}
