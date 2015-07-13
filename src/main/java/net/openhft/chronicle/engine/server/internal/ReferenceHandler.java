package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.pubsub.Reference;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static net.openhft.chronicle.engine.server.internal.PublisherHandler.Params.message;
import static net.openhft.chronicle.engine.server.internal.ReferenceHandler.EventId.get;
import static net.openhft.chronicle.engine.server.internal.ReferenceHandler.EventId.set;
import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class ReferenceHandler<E> extends AbstractHandler {
    private final StringBuilder eventName = new StringBuilder();

    private WireOutPublisher publisher;
    private Reference<E> view;
    @Nullable
    private Function<ValueIn, E> wireToE;

    private BiConsumer<ValueOut, E> vToWire;

    private final BiConsumer<WireIn, Long> dataConsumer = new BiConsumer<WireIn, Long>() {

        @Override
        public void accept(@NotNull final WireIn inWire, Long inputTid) {

            eventName.setLength(0);
            final ValueIn valueIn = inWire.readEventName(eventName);

            if (set.contentEquals(eventName)) {
                view.set((E)valueIn.object(view.getType()));
                return;
            }

            outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

            writeData(inWire.bytes(), out -> {

                if (get.contentEquals(eventName)) {
                    vToWire.accept(outWire.writeEventName(reply), view.get());
                    return;
                }

            });
        }
    };

    void process(@NotNull final WireIn inWire,
                 final WireOutPublisher publisher,
                 final long tid,
                 Reference view, final Wire outWire,
                 final @NotNull WireAdapter wireAdapter) {
        this.vToWire = wireAdapter.valueToWire();
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
        set,
        get,
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
