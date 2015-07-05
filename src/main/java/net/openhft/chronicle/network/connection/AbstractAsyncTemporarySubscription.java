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
     */
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp) {
        super(hub, csp);
    }


    /**
     * @param hub handles the tcp connectivity.
     * @param csp the url of the subscription.
     * @param id  use as a seed to the tid, makes unique tid'd makes reading the logs easier.
     */
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp, byte id) {
        super(hub, csp, id);
    }
}
