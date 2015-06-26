package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.wire.WireIn;

public interface AsyncSubscription {

    /**
     * returns the unique tid that will be used in the subscription, this tid must be unique per
     * socket connection
     *
     * @return the unique tid
     */
    long tid();

    /**
     * Implement this to establish a subscription with the server
     */
    void applySubscribe();

    /**
     * Implement this to consume the subscription
     * @param inWire the wire to write the subscription to
     */
    void onConsumer(final WireIn inWire);

    /**
     * called when the socket connection is closed
     */
    void onClose();

}
