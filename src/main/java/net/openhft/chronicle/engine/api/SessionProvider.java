package net.openhft.chronicle.engine.api;

/**
 * Created by peter on 01/06/15.
 */
public interface SessionProvider {
    SessionDetails get();

    void set(SessionDetails sessionDetails);

    void remove();
}
