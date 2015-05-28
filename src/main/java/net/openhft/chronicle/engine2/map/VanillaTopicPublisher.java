package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

/**
 * Created by peter on 23/05/15.
 */
public class VanillaTopicPublisher<T, M> implements TopicPublisher<T, M> {
    private final Class<T> tClass;
    private final Class<M> mClass;
    private Asset asset;
    private KeyValueStore<T, M, M> underlying;

    public VanillaTopicPublisher(FactoryContext<KeyValueStore<T, M, M>> context) {
        this(context.parent(), context.type(), context.type2(), context.item());
    }

    VanillaTopicPublisher(Asset asset, Class<T> tClass, Class<M> mClass, KeyValueStore<T, M, M> underlying) {
        this.asset = asset;
        this.tClass = tClass;
        this.mClass = mClass;
        this.underlying = underlying;
    }

    @Override
    public View forSession(LocalSession session, Asset asset) {
        return new VanillaTopicPublisher(asset, tClass, mClass, View.forSession(underlying, session, asset));
    }

    @Override
    public void publish(T topic, M message) {
        underlying.put(topic, message);
    }

    @Override
    public void asset(Asset asset) {
        this.asset = asset;
    }

    @Override
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(KeyValueStore<T, M, M> underlying) {
        this.underlying = underlying;
    }

    @Override
    public KeyValueStore<T, M, M> underlying() {
        return underlying;
    }

    @Override
    public void registerSubscriber(TopicSubscriber<T, M> topicSubscriber) {
        asset.registerTopicSubscriber(tClass, mClass, topicSubscriber, "bootstrap=true");
    }
}
