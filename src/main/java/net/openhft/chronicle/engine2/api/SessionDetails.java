package net.openhft.chronicle.engine2.api;

import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;

/**
 * Session local details stored here.
 * <p>
 * Created by peter on 01/06/15.
 */
public interface SessionDetails {
    @Nullable
    String userId();

    @Nullable
    String securityToken();

    @Nullable
    InetSocketAddress connectionAddress();

    long connectTimeMS();

    <I> void set(Class<I> infoClass, I info);

    <I> I get(Class<I> infoClass);
}
