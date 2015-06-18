package net.openhft.chronicle.network.connection;

import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
public interface SocketConnectionProvider {
    SocketChannel lazyConnect();

    SocketChannel reConnect();
}
