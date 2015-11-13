package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.util.SerializableBiFunction;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static net.openhft.chronicle.engine.server.internal.ReferenceHandler.EventId.*;
import static net.openhft.chronicle.engine.server.internal.ReferenceHandler.Params.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class ReferenceHandler<E, T> extends AbstractHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReferenceHandler.class);
    private final StringBuilder eventName = new StringBuilder();
    private final Map<Long, Object> tidToListener = new ConcurrentHashMap<>();
    private WireOutPublisher publisher;
    private Reference<E> view;
    private StringBuilder csp;
    private BiConsumer<ValueOut, E> vToWire;


    @Nullable
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            if (set.contentEquals(eventName)) {
                view.set((E) valueIn.object(view.getType()));
                return;
            }

            if (remove.contentEquals(eventName)) {
                view.remove();
                return;
            }

            if (update2.contentEquals(eventName)) {
                valueIn.marshallable(wire -> {
                    final Params[] params = update2.params();
                    final SerializableBiFunction<E, T, E> updater = (SerializableBiFunction) wire.read(params[0]).object(Object.class);
                    final Object arg = wire.read(params[1]).object(Object.class);
                    view.asyncUpdate(updater, (T) arg);
                });
                return;
            }

            if (registerSubscriber.contentEquals(eventName)) {

                final Reference<E> key = view;
                final Subscriber listener = new Subscriber() {
                    @Override
                    public void onMessage(final Object message) throws InvalidSubscriberException {
                        publisher.put(key, publish -> {
                            publish.writeDocument(true, wire -> wire.writeEventName(tid).int64
                                    (inputTid));
                            publish.writeNotReadyDocument(false, wire -> wire.writeEventName(reply)
                                    .marshallable(m -> m.write(Params.message).object(message)));
                        });
                    }

                    public void onEndOfSubscription() {
                        if (!publisher.isClosed()) {
                            publisher.put(null, publish -> {
                                publish.writeDocument(true, wire ->
                                        wire.writeEventName(tid).int64(inputTid));
                                publish.writeDocument(false, wire ->
                                        wire.writeEventName(EventId.onEndOfSubscription).text(""));
                            });
                        }
                    }
                };

                int p = csp.indexOf("bootstrap=");
                boolean bootstrap = true;
                if (p != -1) {
                    char e = csp.charAt(p + 10);
                    if ('f' == e) bootstrap = false;
                }

                tidToListener.put(inputTid, listener);
                view.registerSubscriber(bootstrap, requestContext.throttlePeriodMs(), listener);
                return;
            }

            if (unregisterSubscriber.contentEquals(eventName)) {
                long subscriberTid = valueIn.int64();
                Subscriber<E> listener = (Subscriber) tidToListener.remove(subscriberTid);
                if (listener == null) {
                    LOG.warn("No subscriber to present to unregisterSubscriber (" + subscriberTid + ")");
                    return;
                }
                view.unregisterSubscriber(listener);
                return;
            }


            outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

            writeData(inWire.bytes(), out -> {

                if (get.contentEquals(eventName)) {
                    vToWire.accept(outWire.writeEventName(reply), view.get());
                    return;
                }

                if (getAndSet.contentEquals(eventName)) {
                    vToWire.accept(outWire.writeEventName(reply), view.getAndSet((E) valueIn.object(view.getType())));
                    return;
                }

                if (getAndRemove.contentEquals(eventName)) {
                    vToWire.accept(outWire.writeEventName(reply), view.getAndRemove());
                    return;
                }

                if (countSubscribers.contentEquals(eventName)) {
                    outWire.writeEventName(reply).int64(view.subscriberCount());
                    return;
                }

                if (update4.contentEquals(eventName)) {
                    valueIn.marshallable(wire -> {
                        final Params[] params = update4.params();
                        final SerializableBiFunction updater = (SerializableBiFunction) wire.read(params[0]).object(Object.class);
                        final Object updateArg = wire.read(params[1]).object(Object.class);
                        final SerializableBiFunction returnFunction = (SerializableBiFunction) wire.read(params[2]).object(Object.class);
                        final Object returnArg = wire.read(params[3]).object(Object.class);
                        outWire.writeEventName(reply).object(view.syncUpdate(updater, updateArg, returnFunction, returnArg));
                    });
                    return;
                }

                valueIn.marshallable(wire -> {
                    final Params[] params = applyTo2.params();
                    final SerializableBiFunction function = (SerializableBiFunction) wire.read(params[0]).object(Object.class);
                    final Object arg = wire.read(params[1]).object(Object.class);
                    outWire.writeEventName(reply).object(view.applyTo(function, arg));
                });

            });
        }
    };


    @Override
    protected void unregisterAll() {
        tidToListener.forEach((k, listener) -> view.unregisterSubscriber((Subscriber) listener));
        tidToListener.clear();
    }

    void process(@NotNull final WireIn inWire,
                 final RequestContext requestContext,
                 final WireOutPublisher publisher,
                 final long tid,
                 Reference view,
                 StringBuilder csp,
                 final Wire outWire,
                 final @NotNull WireAdapter wireAdapter) {
        this.csp = csp;
        this.vToWire = wireAdapter.valueToWire();
        this.requestContext = requestContext;
        this.publisher = publisher(publisher);

        setOutWire(outWire);
        this.outWire = outWire;
        this.view = view;
        dataConsumer.accept(inWire, tid);
    }

    public enum Params implements WireKey {
        value,
        function,
        updateFunction,
        updateArg,
        arg,
        message
    }

    public enum EventId implements ParameterizeWireKey {
        set,
        get,
        remove,
        getAndRemove,
        applyTo2(function, arg),
        update2(function, arg),
        update4(updateFunction, updateArg, function, arg),
        getAndSet(value),
        asyncUpdate,
        registerSubscriber,
        unregisterSubscriber,
        countSubscribers,
        onEndOfSubscription;

        private final WireKey[] params;

        <P extends WireKey> EventId(P... params) {
            this.params = params;
        }

        @NotNull
        public <P extends WireKey> P[] params() {
            return (P[]) this.params;
        }
    }

}
