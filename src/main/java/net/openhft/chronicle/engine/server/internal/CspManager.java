package net.openhft.chronicle.engine.server.internal;

import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
interface CspManager {

    long acquireCid(@NotNull CharSequence csp);

    void storeObject(long cid, Object object);

    void removeCid(long cid);

    long createProxy(String type);

    long createProxy(String type, long token);
}
