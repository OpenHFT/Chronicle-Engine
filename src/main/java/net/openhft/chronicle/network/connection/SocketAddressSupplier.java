package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.network.TCPRegistry;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Provides support for failover TCP connections to the server, if the primary connection can not be
 * estblish, after retrying upto a timeout,  see {@link SocketAddressSupplier#timoutMS()} the other
 * connections will be attempted see, the order of these conneciton is determined by the order of
 * the  {@link SocketAddressSupplier#descriptions}
 *
 * @author Rob Austin.
 */
public class SocketAddressSupplier implements Supplier<SocketAddress> {


    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SocketAddressSupplier.class);
    private final List<SocketAddress> remoteAddresses = new ArrayList<>();
    private final String descriptions;
    private long failoverTimeout = Integer.getInteger("tcp.failover.time", 2000);
    private SocketAddress remoteAddress;

    /**
     * @param connectURIs the connections defined in order with the primary first
     */
    public SocketAddressSupplier(@NotNull final String[] connectURIs) {

        // for (String descriptions : descriptions) {
        this.remoteAddresses.add(TCPRegistry.lookup(connectURIs[0]));
        remoteAddress = remoteAddresses.get(0);
        //  }

        this.descriptions = connectURIs[0];
    }


    public void failoverToNextAddress() {
        LOG.warn("failing over to next address");
    }

    public void startAtFirstAddress() {

    }

    public long timoutMS() {
        return failoverTimeout;
    }

    private String description() {
        return descriptions;
    }

    @Override
    public SocketAddress get() {
        return remoteAddress;
    }

    public String toString() {

        if (remoteAddress == null)
            return "null";

        return remoteAddress.toString().replaceAll("0:0:0:0:0:0:0:0", "localhost") + " - " +
                description();
    }

}
