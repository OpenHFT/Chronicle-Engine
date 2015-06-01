package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.*;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionProvider implements SessionProvider, View {
    private ThreadLocal<SessionDetails> sessionDetails = new ThreadLocal<>();

    public VanillaSessionProvider(Asset asset) {

    }

    @Override
    public SessionDetails get() {
        SessionDetails sessionDetails = this.sessionDetails.get();
        if (sessionDetails == null)
            this.sessionDetails.set(sessionDetails = new VanillaSessionDetails());
        return sessionDetails;
    }

    @Override
    public void set(SessionDetails sessionDetails) {
        this.sessionDetails.set(sessionDetails);
    }

    @Override
    public void remove() {
        sessionDetails.remove();
    }

    @Override
    public void registerSubscriber(Subscriber<SessionDetails> subscriber) {
        throw new UnsupportedOperationException("todo");
    }
}
