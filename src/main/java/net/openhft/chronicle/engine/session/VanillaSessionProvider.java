package net.openhft.chronicle.engine.session;

import net.openhft.chronicle.engine.api.SessionDetails;
import net.openhft.chronicle.engine.api.session.SessionProvider;
import net.openhft.chronicle.engine.api.tree.View;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 01/06/15.
 */
public class VanillaSessionProvider implements SessionProvider, View {
    @NotNull
    private ThreadLocal<SessionDetails> sessionDetails = new ThreadLocal<>();

    public VanillaSessionProvider() {

    }

    @Override
    public SessionDetails get() {
        return this.sessionDetails.get();
    }

    @Override
    public void set(SessionDetails sessionDetails) {
        this.sessionDetails.set(sessionDetails);
    }

    @Override
    public void remove() {
        sessionDetails.remove();
    }
}
