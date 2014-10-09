package net.openhft.chronicle.map;

import java.util.Set;

/**
 * Created by peter on 09/10/14.
 */
public interface ChronicleSet<E> extends Set<E> {
    public long longSize();
}
