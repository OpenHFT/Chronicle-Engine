package net.openhft.chronicle.engine.map.remote;

import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.IndexQuery;
import net.openhft.chronicle.engine.api.query.IndexQueueView;
import net.openhft.chronicle.engine.api.query.IndexedEntry;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.engine.server.internal.MapWireHandler;
import net.openhft.chronicle.network.connection.AbstractAsyncSubscription;
import net.openhft.chronicle.network.connection.AbstractStatelessClient;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.chronicle.engine.server.internal.IndexQueueViewHandler.EventId.*;
import static net.openhft.chronicle.network.connection.CoreFields.reply;

/**
 * @author Rob Austin.
 */
public class RemoteIndexQueueView<K extends Marshallable, V extends Marshallable> extends
        AbstractStatelessClient<MapWireHandler.EventId>
        implements IndexQueueView<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteIndexQueueView.class);
    private final Map<Object, Long> subscribersToTid = new ConcurrentHashMap<>();

    public RemoteIndexQueueView(@NotNull final RequestContext context,
                                @NotNull Asset asset) {
        super(asset.findView(TcpChannelHub.class), (long) 0, toUri(context));
    }

    private static String toUri(@NotNull final RequestContext context) {
        return context.viewType(IndexQueueView.class).toUri();
    }

    @Override
    public void registerSubscriber(@NotNull Subscriber<IndexedEntry<K, V>> subscriber,
                                   @NotNull IndexQuery vanillaIndexQuery) {

        if (hub.outBytesLock().isHeldByCurrentThread())
            throw new IllegalStateException("Cannot view map while debugging");

        hub.subscribe(new AbstractAsyncSubscription(hub, csp, "Remove KV Subscription registerTopicSubscriber") {

                          @Override
                          public void onSubscribe(@NotNull final WireOut wireOut) {
                              subscribersToTid.put(subscriber, tid());
                              wireOut.writeEventName(registerSubscriber)
                                      .typedMarshallable(vanillaIndexQuery);
                          }

                          @Override
                          public void onConsumer(@NotNull final WireIn inWire) {

                              try (DocumentContext dc = inWire.readingDocument()) {
                                  if (!dc.isPresent())
                                      return;

                                  System.out.println(Wires.fromSizePrefixedBlobs(dc.wire().bytes(), dc.wire().bytes().readPosition() -
                                          4));

                                  StringBuilder sb = Wires.acquireStringBuilder();
                                  ValueIn valueIn = dc.wire().readEventName(sb);

                                  if (reply.contentEquals(sb))
                                      try {
                                          IndexedEntry<K, V> e = valueIn.typedMarshallable();
                                          subscriber.onMessage(e);
                                      } catch (InvalidSubscriberException e) {
                                          RemoteIndexQueueView.this.unregisterSubscriber(subscriber);
                                      }
                                  else if (onEndOfSubscription.contentEquals(sb)) {
                                      subscriber.onEndOfSubscription();
                                      hub.unsubscribe(tid());
                                  }
                              } catch (Exception e) {
                                  LOG.error("", e);
                              }

                          }
                      }
        );


    }

    @Override
    public void unregisterSubscriber(@NotNull Subscriber<IndexedEntry<K, V>> listener) {
        Long tid = subscribersToTid.get(listener);

        if (tid == null) {
            LOG.warn("There is no subscription to unsubscribe, was " + subscribersToTid.size() + " other subscriptions.");
            return;
        }

        hub.preventSubscribeUponReconnect(tid);

        if (!hub.isOpen()) {
            hub.unsubscribe(tid);
            return;
        }

        hub.lock(() -> {
            writeMetaDataForKnownTID(tid);
            hub.outWire().writeDocument(false, wireOut -> {
                wireOut.writeEventName(unregisterSubscriber).text("");
            });
        });

    }
}
