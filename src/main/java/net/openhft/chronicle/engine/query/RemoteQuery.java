package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.Query;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static java.util.EnumSet.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.BOOTSTRAP;
import static net.openhft.chronicle.engine.api.tree.RequestContext.Operation.END_SUBSCRIPTION_AFTER_BOOTSTRAP;
import static net.openhft.chronicle.engine.query.Operation.OperationType.*;

/**
 * @author Rob Austin.
 */
public class RemoteQuery<E> implements Query<E> {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteQuery.class);
    private final MapView<E, ?> mapView;
    private final Filter<E> filter = new Filter<>();

    public RemoteQuery(@NotNull MapView<E, ?> mapView) {
        this.mapView = mapView;
    }

    @Override
    public Query<E> filter(SerializablePredicate<? super E> predicate) {
        filter.add(predicate, FILTER);
        return this;
    }

    @Override
    public <R> Query<R> map(SerializableFunction<? super E, ? extends R> mapper) {
        filter.add(mapper, MAP);
        return (Query<R>) this;
    }

    @Override
    public <R> Query<R> project(Class<R> rClass) {
        filter.add(rClass, PROJECT);
        return (Query<R>) this;
    }

    @Override
    public <R> Query<R> flatMap(SerializableFunction<? super E, ? extends Query<? extends R>> mapper) {
        filter.add(mapper, FLAT_MAP);
        return (Query<R>) this;
    }

    @Override
    public Stream<E> stream() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public void subscribe(Consumer<? super E> action) {
        mapView.registerKeySubscriber(action::accept);
    }

    @Override
    public void forEach(Consumer<? super E> action) {

        final BlockingQueue<E> queue = new ArrayBlockingQueue<E>(128);
        final AtomicBoolean finished = new AtomicBoolean();
        final Thread thread = Thread.currentThread();
        final Subscriber<E> accept = new Subscriber<E>() {

            @Override
            public void onMessage(E o) throws InvalidSubscriberException {
                try {
                    final boolean offer = queue.offer(o, 20, SECONDS);

                    synchronized (queue) {
                        queue.notifyAll();
                    }

                    if (!offer) {
                        LOG.error("Queue Full");
                        dumpThreads();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onEndOfSubscription() {
                finished.set(true);
                synchronized (queue) {
                    queue.notifyAll();
                }
            }
        };

        mapView.registerKeySubscriber(
                accept,
                filter,
                of(BOOTSTRAP, END_SUBSCRIPTION_AFTER_BOOTSTRAP));


        while (!finished.get()) {
            try {

                final E message = queue.poll();
                if (message == null) {
                    synchronized (queue) {
                        queue.wait(1000); // 1 SECONDS
                    }
                    continue;
                }

                action.accept(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        queue.forEach(action::accept);


    }

    private void dumpThreads() {
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            Thread thread = entry.getKey();
            if (thread.getThreadGroup().getName().equals("system"))
                continue;
            StringBuilder sb = new StringBuilder();
            sb.append(thread).append(" ").append(thread.getState());
            Jvm.trimStackTrace(sb, entry.getValue());
            sb.append("\n");
            LOG.error("\n========= THREAD DUMP =========\n", sb);
        }
    }

    @Override
    public <R, A> R collect(Collector<? super E, A, R> collector) {
        final A container = collector.supplier().get();
        forEach(o -> collector.accumulator().accept(container, o));
        return collector.finisher().apply(container);
    }

}
