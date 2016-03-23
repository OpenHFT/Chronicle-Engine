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

package net.openhft.chronicle.engine.query;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.util.SerializableFunction;
import net.openhft.chronicle.core.util.SerializablePredicate;
import net.openhft.chronicle.engine.api.pubsub.InvalidSubscriberException;
import net.openhft.chronicle.engine.api.pubsub.Subscriber;
import net.openhft.chronicle.engine.api.query.Query;
import net.openhft.chronicle.engine.api.query.Subscription;
import net.openhft.chronicle.engine.api.query.SubscriptionNotSupported;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
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

/**
 * @author Rob Austin.
 */
public class RemoteQuery<E> implements Query<E> {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteQuery.class);
    private final Filter<E> filter = new Filter<>();
    private final Subscribable<E> subscribable;

    public RemoteQuery(final Subscribable<E> eSubscribable) {
        this.subscribable = eSubscribable;
    }

    @Override
    public Query<E> filter(SerializablePredicate<? super E> predicate) {
        filter.addFilter(predicate);
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> Query<R> map(SerializableFunction<? super E, ? extends R> mapper) {
        filter.addMap(mapper);
        return (Query<R>) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> Query<R> project(Class<R> rClass) {
        filter.addProject(rClass);
        return (Query<R>) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> Query<R> flatMap(SerializableFunction<? super E, ? extends Query<? extends R>> mapper) {
        filter.addFlatMap(mapper);
        return (Query<R>) this;
    }

    @Override
    public Stream<E> stream() {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Subscription subscribe(Consumer<? super E> action) {
        subscribable.subscribe(
                action::accept,
                filter,
                of(BOOTSTRAP));
        return SubscriptionNotSupported.INSTANCE;
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        forEach2(action);
    }

    private void forEach2(Consumer<? super E> action) {
        final BlockingQueue<E> queue = new ArrayBlockingQueue<E>(128);
        final AtomicBoolean finished = new AtomicBoolean();

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

        subscribable.subscribe(
                accept,
                filter,
                of(BOOTSTRAP, END_SUBSCRIPTION_AFTER_BOOTSTRAP));

        while (!finished.get()) {
            try {

                final E message = queue.poll();
                if (message == null) {
                    //noinspection SynchronizationOnLocalVariableOrMethodParameter
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

    public interface Subscribable<E> {
        void subscribe(@NotNull Subscriber<E> subscriber,
                       @NotNull Filter<E> filter,
                       @NotNull Set<RequestContext.Operation> contextOperations);
    }
}
