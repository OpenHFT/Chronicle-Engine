package net.openhft.chronicle.network.connection;

import org.jetbrains.annotations.Nullable;

import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
interface SocketChannelProvider {
    @Nullable
    SocketChannel lazyConnect();

    @Nullable
    SocketChannel reConnect() throws InterruptedException;
}