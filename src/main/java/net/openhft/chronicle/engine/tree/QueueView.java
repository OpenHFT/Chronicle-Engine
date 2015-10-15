package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public interface QueueView<E> extends ChronicleQueue {
    ExcerptTailer theadLocalTailer();

    ExcerptAppender threadLocalAppender();

    void threadLocalElement(E e);

    E threadLocalElement();

    E get(int index);

    E get();

    void set(E event);

    @Override
    long size();

    @Override
    void clear();

    @Override
    long firstAvailableIndex();

    @Override
    long lastWrittenIndex();

    @Override
    void close() throws IOException;
}
