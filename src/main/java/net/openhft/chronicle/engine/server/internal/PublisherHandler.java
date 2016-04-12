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

import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.publish;
import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.registerSubscriber;
import static net.openhft.chronicle.engine.server.internal.PublisherHandler.Params.message;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class PublisherHandler<E> extends AbstractHandler {
    private final StringBuilder eventName = new StringBuilder();

    private WireOutPublisher publisher;
    private Publisher<E> view;
    @Nullable
    private Function<ValueIn, E> wireToE;
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            if (registerSubscriber.contentEquals(eventName)) {
                final Object key = view;
                final Subscriber listener = message -> {
                    synchronized (publisher) {
                        publisher.put(key, publish -> {

                            publish.writeDocument(true, wire -> wire.writeEventName(tid).int64
                                    (inputTid));
                            publish.writeNotCompleteDocument(false, wire -> wire.writeEventName(reply)
                                    .marshallable(m -> m.write(Params.message).object(message)));
                        });
                    }
                };

                // TODO CE-101 get the true value from the CSP
                boolean bootstrap = true;

                valueIn.marshallable(m -> view.registerSubscriber(bootstrap,
                        requestContext.throttlePeriodMs(), listener));
                return;
            }

            if (publish.contentEquals(eventName)) {

                valueIn.marshallable(w -> {
                    final Params[] params = publish.params();

                    final Params param = params[0];
                    final ValueIn read = w.read(param);
                    final E message = wireToE.apply(read);

                    nullCheck(message);
                    view.publish(message);
                });

            }

        }
    };

    void process(@NotNull final WireIn inWire,
                 @NotNull final RequestContext requestContext,
                 @NotNull final WireOutPublisher publisher,
                 final long tid,
                 @NotNull final Publisher view,
                 @NotNull final Wire outWire,
                 @NotNull final WireAdapter wireAdapter) {
        setOutWire(outWire);
        this.outWire = outWire;
        this.publisher = publisher;
        this.view = view;
        this.wireToE = wireAdapter.wireToValue();
        this.requestContext = requestContext;
        dataConsumer.accept(inWire, tid);
    }

    public enum Params implements WireKey {
        message
    }

    public enum EventId implements ParameterizeWireKey {
        publish(message),
        onEndOfSubscription,
        registerSubscriber(message);

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
