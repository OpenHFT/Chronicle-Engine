package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.tree.AssetNotFoundException;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.tree.TopologySubscription;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static net.openhft.chronicle.network.connection.CoreFields.reply;
import static net.openhft.chronicle.network.connection.CoreFields.tid;

/**
 * Created by Rob Austin
 */
public class TopologySubscriptionHandler extends SubscriptionHandler<TopologySubscription> {

    @Nullable
    private final BiConsumer<WireIn, Long> dataConsumer = (inWire, inputTid) -> {

        eventName.setLength(0);
        final ValueIn valueIn = inWire.readEventName(eventName);

        try {
            if (before(inputTid, valueIn)) return;
        } catch (AssetNotFoundException e) {
            throw new AssertionError(e);
        }

        outWire.writeDocument(true, wire -> outWire.writeEventName(tid).int64(inputTid));

        writeData(inWire.bytes(), out -> {

            if (after(eventName)) return;

            if (EventId.notifyEvent.contentEquals(eventName)) {
                subscription.notifyEvent(valueIn.typedMarshallable());
                outWire.writeEventName(reply).int8(subscription.entrySubscriberCount());
            }

        });
    };

    void process(@NotNull final WireIn inWire,
                 final RequestContext requestContext,
                 final Queue<Consumer<Wire>> publisher,
                 final AssetTree assetTree, final long tid,
                 final Wire outWire, final TopologySubscription subscription) {
        setOutWire(outWire);
        this.outWire = outWire;
        this.subscription = subscription;
        this.requestContext = requestContext;
        this.publisher = publisher;
        this.assetTree = assetTree;
        dataConsumer.accept(inWire, tid);

    }

    public enum EventId implements ParameterizeWireKey {
        notifyEvent;

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
