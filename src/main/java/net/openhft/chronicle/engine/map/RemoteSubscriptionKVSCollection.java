package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.engine.api.*;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapReplicationEvent;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpConnectionHub;
import org.jetbrains.annotations.NotNull;

/**
 * Created by daniel on 10/06/15.
 */
public class RemoteSubscriptionKVSCollection<K,MV,V> extends AbstractStatelessClient implements SubscriptionKVSCollection<K, MV, V> {


    private RemoteAuthenticatedKeyValueStore<K, V> kvStore;

    public RemoteSubscriptionKVSCollection(RemoteAuthenticatedKeyValueStore<K, V> kvStore, RequestContext context, Asset asset) {
        super(TcpConnectionHub.hub(context, asset), (long) 0, toUri(context));
        this.kvStore = kvStore;
    }

    @Override
    public boolean needsPrevious() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> store) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void notifyEvent(MapReplicationEvent<K, V> mpe) throws InvalidSubscriberException {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public <E> void registerSubscriber(RequestContext rc, Subscriber<E> subscriber) {
        
    }

    @Override
    public <T, E> void registerTopicSubscriber(RequestContext rc, TopicSubscriber<T, E> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterSubscriber(RequestContext rc, Subscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterTopicSubscriber(RequestContext rc, TopicSubscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerDownstream(Subscription subscription) {
        throw new UnsupportedOperationException("todo");
    }

    @NotNull
    private static String toUri(@NotNull final RequestContext context) {
        return "/" + context.name()
                + "?view=" + "map&keyType=" + context.keyType().getSimpleName() + "&valueType=" + context.valueType()
                .getSimpleName();
    }
}
