package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.*;
import net.openhft.chronicle.engine2.api.map.SubscriptionKeyValueStore;
import net.openhft.chronicle.engine2.session.LocalSession;

import java.util.function.Supplier;

/**
 * Created by peter on 23/05/15.
 */
public class VanillaTopicPublisher<T, M> implements TopicPublisher<T, M> {
    private final Class<T> tClass;
    private final Class<M> mClass;
    private Asset asset;
    private SubscriptionKeyValueStore<T, M, M> underlying;

    public VanillaTopicPublisher(RequestContext context, Asset asset, Supplier<Assetted> assetted) {
        this(asset, context.type(), context.type2(), (SubscriptionKeyValueStore<T, M, M>) assetted.get());
    }

    VanillaTopicPublisher(Asset asset, Class<T> tClass, Class<M> mClass, SubscriptionKeyValueStore<T, M, M> underlying) {
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
    public Asset asset() {
        return asset;
    }

    @Override
    public void underlying(SubscriptionKeyValueStore<T, M, M> underlying) {
        this.underlying = underlying;
    }

    @Override
    public SubscriptionKeyValueStore<T, M, M> underlying() {
        return underlying;
    }

    @Override
    public void registerSubscriber(TopicSubscriber<T, M> topicSubscriber) {
        asset.subscription(true).registerTopicSubscriber(RequestContext.requestContext().bootstrap(true).type(tClass).type2(mClass), topicSubscriber);
    }
}
