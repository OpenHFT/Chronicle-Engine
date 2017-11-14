package net.openhft.chronicle.engine;

import net.openhft.chronicle.network.connection.TcpChannelHub;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.Closeable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ShutdownHooks implements TestRule {
    private final Queue<Closeable> closeables = new ConcurrentLinkedQueue<>();
    private final Queue<ExecutorService> executors = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public <T extends Closeable> T addCloseable(final T closeable) {
        checkClosed();
        closeables.add(closeable);

        return closeable;
    }

    public <T extends ExecutorService> T shutdownExecutor(final T executor) {
        checkClosed();
        executors.add(executor);

        return executor;
    }


    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (Closeable closeable : closeables) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            for (ExecutorService executor : executors) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            TcpChannelHub.closeAllHubs();
        }
    }

    private void checkClosed() {
        if (closed.get()) {
            final AssertionError error = new AssertionError("Cannot add new Closeable when already closed!");
            error.printStackTrace();
            throw error;
        }
    }

    @Override
    public Statement apply(final Statement statement, final Description description) {
        return new Statement() {
            public void evaluate() throws Throwable {
                try {
                    statement.evaluate();
                } finally {
                    close();
                }

            }
        };
    }
}