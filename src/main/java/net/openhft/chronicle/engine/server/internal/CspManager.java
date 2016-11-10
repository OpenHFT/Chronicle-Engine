package net.openhft.chronicle.engine.server.internal;

import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface CspManager {

    long createCid(@NotNull CharSequence csp);

    void storeObject(long cid, Object object);

    <O> O getObject(long cid);

    void removeCid(long cid);

    long createProxy(String eventName);
}
