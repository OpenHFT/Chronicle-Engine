/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.pubsub.Publisher;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.publish;
import static net.openhft.chronicle.engine.server.internal.PublisherHandler.EventId.registerSubscriber;
import static net.openhft.chronicle.engine.server.internal.PublisherHandler.Params.message;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by rob on 27/06/2015.
 */
public class TopologyHandler<E> extends AbstractHandler {
    private final StringBuilder eventName = new StringBuilder();

    private Queue<Consumer<Wire>> publisher;
    private Publisher<E> view;
    @Nullable
    private Function<ValueIn, E> wireToE;
    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            if (registerSubscriber.contentEquals(eventName)) {

                final Subscriber listener = new Subscriber() {

                    @Override
                    public void onMessage(final Object message) {
                        publisher.add(publish -> {
                            publish.writeDocument(true, wire -> wire.writeEventName(tid).int64
                                    (inputTid));
                            publish.writeNotCompleteDocument(false, wire -> wire.writeEventName(reply)
                                    .marshallable(m -> m.write(Params.message).object(message)));
                        });
                    }

                };

                // TODO CE-101
                boolean bootstrap = true;

                valueIn.marshallable(m -> view.registerSubscriber(bootstrap, requestContext.throttlePeriodMs(), listener));
                return;
            }

            outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

            writeData(inWire , out -> {

                if (publish.contentEquals(eventName)) {

                    valueIn.marshallable(wire -> {
                        final Params[] params = publish.params();

                        final E message = wireToE.apply(wire.read(params[1]));

                        nullCheck(message);
                        view.publish(message);
                    });

                }

            });
        }
    };

    void process(@NotNull final Wire inWire,
                 final Queue<Consumer<Wire>> publisher,
                 final long tid,
                 Publisher<E> view, final Wire outWire,
                 final @NotNull WireAdapter<?, E> wireAdapter) {
        setOutWire(outWire);
        this.outWire = outWire;
        this.publisher = publisher;
        this.view = view;
        this.wireToE = wireAdapter.wireToValue();
        dataConsumer.accept(inWire, tid);

    }

    public enum Params implements WireKey {
        message
    }

    public enum EventId implements ParameterizeWireKey {
        publish,
        onEndOfSubscription,
        registerTopicSubscriber(message);

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