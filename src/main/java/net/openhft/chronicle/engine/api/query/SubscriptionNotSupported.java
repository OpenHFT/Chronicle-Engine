package net.openhft.chronicle.engine.api.query;

/**
 * Created by peter.lawrey on 07/09/2015.
 */
public enum SubscriptionNotSupported implements Subscription {
    INSTANCE;


    @Override
    public void cancel() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean isComplete() {
        throw new UnsupportedOperationException("todo");
    }
}
