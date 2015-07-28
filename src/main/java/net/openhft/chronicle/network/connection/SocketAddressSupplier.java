package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.network.TCPRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Provides support for failover TCP connections to the server, if the primary connection can not be
 * establish, after retrying upto a timeout,  see {@link SocketAddressSupplier#timeoutMS()} the
 * other connections will be attempted see, the order of these connections are determined by the
 * order of the connectURIs
 *
 * @author Rob Austin.
 */
public class SocketAddressSupplier implements Supplier<SocketAddress> {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SocketAddressSupplier.class);
    private final String name;
    private RemoteAddressSupplier current;

    public List<RemoteAddressSupplier> all() {
        return remoteAddresses;
    }

    private class RemoteAddressSupplier implements Supplier<SocketAddress> {

        private final SocketAddress remoteAddress;
        private final String description;

        public RemoteAddressSupplier(String description) {
            this.description = description;
            remoteAddress = TCPRegistry.lookup(description);
        }

        @Override
        public SocketAddress get() {
            return remoteAddress;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private final List<RemoteAddressSupplier> remoteAddresses = new ArrayList<>();

    private long failoverTimeout = Integer.getInteger("tcp.failover.time", 2_000);

    private Iterator<RemoteAddressSupplier> iterator;


    public String name() {
        return name;
    }

    /**
     * @param connectURIs the socket connections defined in order with the primary first
     * @param name        the name of this service
     */
    public SocketAddressSupplier(@NotNull final String[] connectURIs, String name) {

        this.name = name;

        for (String connectURI : connectURIs) {
            this.remoteAddresses.add(new RemoteAddressSupplier(connectURI));
        }

        // for (String descriptions : descriptions) {
        this.iterator = remoteAddresses.iterator();
        next();
    }


    public void failoverToNextAddress() {
        LOG.warn("failing over to next address");
        next();
    }

    private void next() {
        if (iterator.hasNext())
            this.current = iterator.next();
        else
            this.current = null;
    }

    public void startAtFirstAddress() {
        iterator = remoteAddresses.iterator();
        next();
    }

    public long timeoutMS() {
        return failoverTimeout;
    }


    @Nullable
    @Override
    public SocketAddress get() {
        if (current == null)
            return null;
        return current.get();
    }

    public String toString() {

        RemoteAddressSupplier current = this.current;

        if (current == null)
            return "(none)";

        final SocketAddress socketAddress = current.get();
        if (socketAddress == null)
            return "(none)";

        return socketAddress.toString().replaceAll("0:0:0:0:0:0:0:0", "localhost") + " - " +
                current.toString();
    }

}
