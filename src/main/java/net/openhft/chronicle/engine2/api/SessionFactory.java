package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface SessionFactory {
    Session createSession();

    default Session createSession(String hostname, int port) {
        return createSession();
    }

    default Session createSession(String hostname, int port, String username, String password) {
        return createSession();
    }

    default Session createSession(String hostname, int port, String username, byte[] token) {
        return createSession();
    }

}
