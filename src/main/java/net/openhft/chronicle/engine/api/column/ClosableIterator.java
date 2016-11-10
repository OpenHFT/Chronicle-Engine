package net.openhft.chronicle.engine.api.column;

import net.openhft.chronicle.core.io.Closeable;

import java.util.Iterator;

/**
 * create for remote access, ( from a remote client ), call close when this iterator is
 * no-longer required
 *
 * @author Rob Austin.
 */
public interface ClosableIterator<E> extends Iterator<E>, Closeable {
}
