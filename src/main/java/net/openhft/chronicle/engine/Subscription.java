package net.openhft.chronicle.engine;

public interface Subscription<K, C> {
    public void subscribeAll();

    public void subscribe(K... keys);

    public void unsubscribeAll();

    public void unsubscribe(K... keys);

    public void setCallback(C callback);
}