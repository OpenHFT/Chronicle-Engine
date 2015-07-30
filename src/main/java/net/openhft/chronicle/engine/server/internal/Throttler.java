package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.threads.HandlerPriority;
import net.openhft.chronicle.threads.api.EventHandler;
import net.openhft.chronicle.threads.api.EventLoop;
import net.openhft.chronicle.threads.api.InvalidEventHandlerException;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This code focusing on maintain message order, It attempts not to exceed the {@code
 * maxEventsPreSecond}, in other word in rare case a few more message maybe send in any second that
 * the {@code maxEventsPreSecond } this is due to the use of lazySet() amongst other things
 *
 * @author Rob Austin.
 */
public class Throttler<K> implements EventHandler {
    private final int maxEventsPreSecond;
    private final AtomicLong numberOfMessageSent = new AtomicLong(0);
    private final ConcurrentLinkedQueue<Runnable> events = new ConcurrentLinkedQueue<>();
    private long lastKnownSeconds = System.currentTimeMillis();
    private final ReentrantLock lock = new ReentrantLock();

    public Throttler(@NotNull final EventLoop eventLoop, int maxEventsPreSecond) {
        this.maxEventsPreSecond = maxEventsPreSecond;
        eventLoop.addHandler(this);
    }

    public void add(Runnable r) {
        events.add(r);
        sendEvents();
    }

    /**
     * sends all the events in the queue up to the maxEventsPreSecond
     */
    private void sendEvents() {

        if (this.numberOfMessageSent.get() < maxEventsPreSecond) {

            // using the lock ensures that the messages are sent in order
            lock.lock();
            try {
                send();
            } finally {
                lock.unlock();
            }
        }

    }


    /**
     * tries to sends all the events in the queue up to the maxEventsPreSecond, unless the lock is
     * already held, then on action is taken
     */
    private void trySendEvents() {
        if (this.numberOfMessageSent.get() < maxEventsPreSecond) {

            // using the lock ensures that the messages are sent in order
            if (lock.tryLock()) {
                try {
                    send();
                } finally {
                    lock.unlock();
                }
            }

        }
    }

    private void send() {
        long value = this.numberOfMessageSent.get();
        while (value < maxEventsPreSecond) {

            final Runnable r = events.poll();

            if (r == null)
                break;

            r.run();
            value = lazyIncrement();
        }
    }

    /**
     * because this is lazy, this is a approximation ( non atomic ), but its good enough for
     * throttling
     *
     * @return the numberOfMessageSent after its has been incremented
     */
    private long lazyIncrement() {
        final long value = this.numberOfMessageSent.get() + 1;
        this.numberOfMessageSent.lazySet(value);
        return value;
    }

    @NotNull
    @Override
    public HandlerPriority priority() {
        return HandlerPriority.MONITOR;
    }

    @Override
    public boolean action() throws InvalidEventHandlerException {

        final long currentSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

        if (currentSeconds != lastKnownSeconds) {

            final int secondsPassed = (int) (currentSeconds - lastKnownSeconds);
            final long v = this.numberOfMessageSent.get() - (secondsPassed * maxEventsPreSecond);

            lastKnownSeconds = currentSeconds;
            this.numberOfMessageSent.lazySet(v <= 0 ? 0 : v);

            // this may not successes if the lock is already held
            trySendEvents();
        }

        return true;

    }
}
