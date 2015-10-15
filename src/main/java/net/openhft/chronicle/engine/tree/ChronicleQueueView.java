package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.api.tree.RequestContext;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.Excerpt;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Rob Austin.
 */
public class ChronicleQueueView<E> implements QueueView<E> {

    private final ChronicleQueue chronicleQueue;
    private final Class<E> type;

    private final ThreadLocal<ThreadLocalData> threadLocal;

    public class ThreadLocalData {

        public final ExcerptAppender appender;
        public final ExcerptTailer tailer;
        public E element;

        public ThreadLocalData(ChronicleQueue chronicleQueue) {
            try {
                appender = chronicleQueue.createAppender();
                tailer = chronicleQueue.createTailer();
            } catch (IOException e) {
                throw Jvm.rethrow(e);
            }
        }
    }

    public ChronicleQueueView(RequestContext requestContext, Asset asset) {
        chronicleQueue = newInstance(requestContext.name(), requestContext.basePath());
        type = requestContext.type();
        threadLocal = ThreadLocal.withInitial(() -> new ThreadLocalData(chronicleQueue));
    }


    private ChronicleQueue newInstance(String fullName, String basePath) {
        ChronicleQueue chronicleQueue;
        File baseFilePath;
        try {

            if (basePath != null) {
                baseFilePath = new File(basePath + fullName);
                //noinspection ResultOfMethodCallIgnored
                baseFilePath.mkdirs();
            } else {
                final Path tempDirectory = Files.createTempDirectory("engine-queue");
                baseFilePath = tempDirectory.toFile();
            }

            chronicleQueue = new SingleChronicleQueueBuilder(baseFilePath).build();
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
        return chronicleQueue;
    }

    @Override
    public String name() {
        return chronicleQueue.name();
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return chronicleQueue.createExcerpt();
    }

    @Override
    public ExcerptTailer theadLocalTailer() {
        return threadLocal.get().tailer;
    }

    @Override
    public ExcerptAppender threadLocalAppender() {
        return threadLocal.get().appender;
    }

    @Override
    public void threadLocalElement(E e) {
        threadLocal.get().element = e;
    }

    @Override
    public E threadLocalElement() {
        return (E) threadLocal.get().element;
    }


    /**
     * @param index gets the except at the given index  or {@code null} if the index is not valid
     * @return the except
     */
    @Override
    public E get(int index) {
        try {
            final ExcerptTailer tailer = theadLocalTailer();
            if (!tailer.index(index))
                return null;
            return tailer.readDocument(
                    wire -> threadLocalElement(wire.read().object(type))) ?
                    threadLocalElement() : null;
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }


    /**
     * @return the last except or {@code null} if there are no more excepts available
     */
    @Override
    public E get() {
        try {
            final ExcerptTailer tailer = theadLocalTailer();
            return tailer.readDocument(
                    wire -> threadLocalElement(wire.read().object(type))) ?
                    threadLocalElement() : null;
        } catch (Exception e) {
            throw Jvm.rethrow(e);
        }
    }


    @Override
    public void set(E event) {
        try {
            threadLocalAppender().writeDocument(w -> w.write().object(event));
        } catch (IOException e) {
            throw Jvm.rethrow(e);
        }
    }


    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return chronicleQueue.createTailer();
    }

    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return chronicleQueue.createAppender();
    }

    @Override
    public long size() {
        return chronicleQueue.size();
    }

    @Override
    public void clear() {
        chronicleQueue.clear();
    }

    @Override
    public long firstAvailableIndex() {
        return chronicleQueue.firstAvailableIndex();
    }

    @Override
    public long lastWrittenIndex() {
        return chronicleQueue.lastWrittenIndex();
    }

    @Override
    public void close() throws IOException {
        chronicleQueue.close();
    }

}
