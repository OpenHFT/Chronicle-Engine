package net.openhft.chronicle.network.connection;

import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
interface SocketChannelProvider {
    SocketChannel lazyConnect();

    SocketChannel reConnect();
}