package net.openhft.chronicle.engine.map;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.map.KeyValueStore;
import net.openhft.chronicle.engine.api.map.MapEvent;
import net.openhft.chronicle.engine.api.map.MapEventListener;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.pubsub.TopicSubscriber;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpConnectionHub;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.subscribe;
import static net.openhft.chronicle.engine.server.internal.MapWireHandler.EventId.unSubscribe;

/**
 * Created by daniel on 10/06/15.
 */
public class RemoteKVSSubscription<K, MV, V> extends AbstractStatelessClient implements ObjectKVSSubscription<K, MV, V>, Closeable {

    private final Class<V> valueType;
    private final ExecutorService eventLoop = Executors.newSingleThreadExecutor(new NamedThreadFactory("RemoteKVSSubscription"));
    private final Class<K> keyType;
    private long subscriberTID = -1;
    private static final Logger LOG = LoggerFactory.getLogger(MapWireHandler.class);
    private volatile boolean closed;

    public RemoteKVSSubscription(RequestContext context, Asset asset) {
        super(TcpConnectionHub.hub(context, asset), (long) 0, toUri(context));
        valueType = context.valueType();
        keyType = context.keyType();
    }

    @Override
    public boolean needsPrevious() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void setKvStore(KeyValueStore<K, MV, V> store) {
    }

    @Override
    public void notifyEvent(MapEvent<K, V> mpe) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerSubscriber(RequestContext rc, Subscriber<MapEvent<K, V>> subscriber) {
        final long startTime = System.currentTimeMillis();

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.outBytesLock().lock();
        try {
            subscriberTID = writeMetaDataStartTime(startTime);
            hub.outWire().writeDocument(false, wireOut -> {
                wireOut.writeEventName(subscribe);
            });

            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }

        assert !hub.outBytesLock().isHeldByCurrentThread();

        // todo a hack - should be fixed !!
        final long timeoutTime = Long.MAX_VALUE;

        eventLoop.execute(() -> {
            // receive
            try {
                while (!closed) {
                    hub.inBytesLock().lock();
                    try {
                        final Wire wire = hub.proxyReply(timeoutTime, subscriberTID);
                        checkIsData(wire);
                        readReplyConsumer(wire, CoreFields.reply, (Consumer<ValueIn>) valueIn -> valueIn.marshallable(r -> onEvent(r, subscriber)));
                    } finally {
                        hub.inBytesLock().unlock();
                    }
                }
            }catch(Throwable t){
                if(!closed && !hub.isClosed()){
                    t.printStackTrace();
                }
            }
        });

    }

    @Override
    public void registerKeySubscriber(RequestContext rc, Subscriber<K> kSubscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterKeySubscriber(Subscriber<K> kSubscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerTopicSubscriber(RequestContext rc, TopicSubscriber<K, V> subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterTopicSubscriber(TopicSubscriber subscriber) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void registerDownstream(EventConsumer<K, V> subscription) {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void unregisterSubscriber(Subscriber<MapEvent<K, V>> subscriber) {
        if(subscriberTID==-1){
            LOG.warn("There is subscription to unsubscribe");
        }

        hub.outBytesLock().lock();
        try {
            writeMetaDataForKnownTID(subscriberTID);
            hub.outWire().writeDocument(false, wireOut -> {
                wireOut.writeEventName(unSubscribe);
            });

            hub.writeSocket(hub.outWire());
        } finally {
            hub.outBytesLock().unlock();
        }
    }

    @NotNull
    private static String toUri(@NotNull final RequestContext context) {
        return "/" + context.name()
                + "?view=" + "map&keyType=" + context.keyType().getName() + "&valueType=" + context.valueType()
                .getName();
    }

    private void onEvent(WireIn r, Subscriber<MapEvent<K, V>> subscriber) {
        byte eventType = r.read().int8();
        K key = r.read(MapWireHandler.Params.key).object(keyType);


        try {
            subscriber.onMessage(new MapEvent<K, V>() {
                @Override
                public void writeMarshallable(WireOut wire) {
                    throw new UnsupportedOperationException("todo");
                }

                @Override
                public void readMarshallable(WireIn wire) throws IllegalStateException {
                    throw new UnsupportedOperationException("todo");
                }

                @Override
                public String assetName() {
                    throw new UnsupportedOperationException("todo");
                }

                @Override
                public K key() {
                    return key;
                }

                @Override
                public V value() {
                    throw new UnsupportedOperationException("todo");
                }

                @Override
                public V oldValue() {
                    throw new UnsupportedOperationException("todo");
                }

                @Override
                public void apply(MapEventListener listener) {
                    if(eventType==1) {
                        V newValue = r.read(MapWireHandler.Params.newValue).object(valueType);
                        listener.insert(key, newValue);
                    }else if(eventType==2){
                        V oldValue = r.read(MapWireHandler.Params.oldValue).object(valueType);
                        V newValue = r.read(MapWireHandler.Params.newValue).object(valueType);
                        listener.update(key, oldValue, newValue);
                    }else if(eventType==3){
                        V oldValue = r.read(MapWireHandler.Params.oldValue).object(valueType);
                        listener.remove(key, oldValue);
                    }else{
                        throw new AssertionError("Event type " + eventType + " not supported");
                    }
                }

                @Override
                public MapEvent translate(BiFunction keyFunction, BiFunction valueFunction) {
                    throw new UnsupportedOperationException("todo");
                }

                @Override
                public MapEvent<K, V> translate(Function keyFunction, Function valueFunction) {
                    throw new UnsupportedOperationException("todo");
                }
            });
        } catch (InvalidSubscriberException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(){
        closed = true;
        eventLoop.shutdown();
    }
}

