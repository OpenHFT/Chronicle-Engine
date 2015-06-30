package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author Rob Austin.
 */
public abstract class AbstractAsyncTemporarySubscription extends AbstractAsyncSubscription
        implements AsyncTemporarySubscription {
    public AbstractAsyncTemporarySubscription(@NotNull TcpChannelHub hub, @Nullable String csp) {
        super(hub, csp);
    }
}
