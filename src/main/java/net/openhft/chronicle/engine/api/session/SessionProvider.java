package net.openhft.chronicle.engine.api.session;

import net.openhft.chronicle.engine.api.SessionDetails;

/**
 * Created by peter on 01/06/15.
 */
public interface SessionProvider {
    SessionDetails get();

    void set(SessionDetails sessionDetails);

    void remove();
}
