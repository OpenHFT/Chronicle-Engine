package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public abstract class AbstractAsyncTemporarySubscription extends AbstractAsyncSubscription
        implements AsyncTemporarySubscription {

    /**
     * @param hub handles the tcp connectivity.
     * @param csp the url of the subscription.
     * @param name the name of the subscription
     */
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, String name) {
        super(hub, csp, name);
    }

    /**
     * @param hub handles the tcp connectivity.
     * @param csp the url of the subscription.
     * @param id  use as a seed to the tid, makes unique tid's makes reading the logs easier.
     * @param name the name of the subscription
     */
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, byte id, String name) {
        super(hub, csp, id, name);
    }
}
