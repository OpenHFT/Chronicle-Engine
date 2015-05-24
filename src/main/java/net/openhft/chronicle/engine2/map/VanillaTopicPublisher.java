package net.openhft.chronicle.engine2.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.FactoryContext;
import net.openhft.chronicle.engine2.api.TopicPublisher;
import net.openhft.chronicle.engine2.api.map.KeyValueStore;

/**
 * Created by peter on 23/05/15.
 */
public class VanillaTopicPublisher<M> implements TopicPublisher<M> {
    private Asset asset;
    private KeyValueStore<String, M> underlying;

    public VanillaTopicPublisher(FactoryContext<KeyValueStore<String, M>> context) {
        this.asset = context.parent();
        this.underlying = context.item();
    }

    @Override
    public void publish(String topic, M message) {
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
    public void underlying(KeyValueStore<String, M> underlying) {
        this.underlying = underlying;
    }

    @Override
    public KeyValueStore<String, M> underlying() {
        return underlying;
    }
}
