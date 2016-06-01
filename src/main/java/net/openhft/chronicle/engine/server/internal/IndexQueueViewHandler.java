/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.engine.api.pubsub.ConsumingSubscriber;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.query.IndexQueueView;
import net.openhft.chronicle.engine.api.query.IndexedValue;
import net.openhft.chronicle.engine.api.query.VanillaIndexQuery;
import net.openhft.chronicle.engine.api.query.VanillaIndexQueueView;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.WireOutConsumer;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static net.openhft.chronicle.engine.server.internal.IndexQueueViewHandler.EventId.registerSubscriber;
import static net.openhft.chronicle.engine.server.internal.IndexQueueViewHandler.EventId.unregisterSubscriber;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class IndexQueueViewHandler<V extends Marshallable> extends AbstractHandler {

    private static final Logger LOG = LoggerFactory.getLogger(IndexQueueViewHandler.class);

    private Asset contextAsset;
    private WireOutPublisher publisher;

    private final StringBuilder eventName = new StringBuilder();
    private final Map<Long, ConsumingSubscriber<IndexedValue<V>>> tidToListener = new ConcurrentHashMap<>();


    @NotNull
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, inputTid) -> {

        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        if (registerSubscriber.contentEquals(eventName)) {
            if (tidToListener.containsKey(tid)) {
                LOG.info("Duplicate topic registration for tid " + tid);
                return;
            }

            final ConsumingSubscriber<IndexedValue<V>> listener = new ConsumingSubscriber<IndexedValue<V>>() {

                volatile WireOutConsumer wireOutConsumer;
                volatile boolean subscriptionEnded;

                @Override
                public void onMessage(IndexedValue indexedEntry) throws InvalidSubscriberException {

                    if (publisher.isClosed())
                        throw new InvalidSubscriberException();

                    publisher.put(indexedEntry.k(), publish -> {
                        publish.writeDocument(true, wire -> wire.writeEventName(tid).int64(inputTid));
                        publish.writeNotCompleteDocument(false, wire ->
                                wire.writeEventName(reply).typedMarshallable(indexedEntry));
                    });
                }

                public void onEndOfSubscription() {
                    subscriptionEnded = true;
                    if (publisher.isClosed())
                        return;
                    publisher.put(null, publish -> {
                        publish.writeDocument(true, wire ->
                                wire.writeEventName(tid).int64(inputTid));
                        publish.writeDocument(false, wire ->
                                wire.writeEventName(ObjectKVSubscriptionHandler.EventId.onEndOfSubscription).text(""));
                    });
                }


                /**
                 * used to publish bytes on the nio socket thread
                 *
                 * @param supplier reads a chronicle queue and
                 *                        publishes writes the data
                 *                        directly to the socket
                 */
                public void addSupplier(Supplier<List<Marshallable>> supplier) {
                    publisher.addWireConsumer(wireOut -> {

                        List<Marshallable> marshallables = supplier.get();
                        if (marshallables == null)
                            return;
                        for (Marshallable marshallable : marshallables) {

                            if (marshallable == null)
                                continue;

                            if (publisher.isClosed())
                                return;

                            wireOut.writeDocument(true, wire -> wire.writeEventName(tid).int64(inputTid));
                            wireOut.writeNotCompleteDocument(false, wire -> {
                                wire.writeEventName(reply).typedMarshallable(marshallable);
                            });
                        }
                    });
                }

                @Override
                public void close() {
                    publisher.removeBytesConsumer(wireOutConsumer);
                }
            };

            final VanillaIndexQuery<V> query = valueIn.typedMarshallable();

            if (query.select().isEmpty() || query.valueClass() == null) {
                LOG.warn("received empty query");
                return;
            }

            try {
                query.filter();
            } catch (Exception e) {
                LOG.error("unable to load the filter predicate for this query=" + query, e);
                return;
            }

            final IndexQueueView<ConsumingSubscriber<IndexedValue<V>>, V> indexQueueView =
                    contextAsset.acquireView(IndexQueueView.class);
            indexQueueView.registerSubscriber(listener, query);
            return;
        }

        if (unregisterSubscriber.contentEquals(eventName)) {

            VanillaIndexQueueView<V> indexQueueView = contextAsset.acquireView(VanillaIndexQueueView.class);
            ConsumingSubscriber<IndexedValue<V>> listener = tidToListener.remove(inputTid);

            if (listener == null) {
                LOG.warn("No subscriber to present to unsubscribe (" + inputTid + ")");
                return;
            }

            if (listener instanceof Closeable)
                listener.close();

            indexQueueView.unregisterSubscriber(listener);
            return;
        }

        outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

    };

    @Override
    protected void unregisterAll() {
        final VanillaIndexQueueView<V> indexQueueView = contextAsset.acquireView(VanillaIndexQueueView.class);
        tidToListener.forEach((k, listener) -> indexQueueView.unregisterSubscriber(listener));
        tidToListener.clear();
    }

    void process(@NotNull final WireIn inWire,
                 @NotNull final RequestContext requestContext,
                 @NotNull Asset contextAsset,
                 @NotNull final WireOutPublisher publisher,
                 final long tid,
                 @NotNull final Wire outWire) {
        setOutWire(outWire);
        this.outWire = outWire;
        this.publisher = publisher;
        this.contextAsset = contextAsset;
        this.requestContext = requestContext;
        dataConsumer.accept(inWire, tid);
    }

    public enum Params implements WireKey {
        subscribe
    }

    public enum EventId implements ParameterizeWireKey {
        registerSubscriber(Params.subscribe),
        unregisterSubscriber(),
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
