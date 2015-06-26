package net.openhft.chronicle.network.connection;

import net.openhft.chronicle.wire.Wire;
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
     * create you subscription here
     */
    void applySubscribe();

    /**
     * you should consumer you subscription here
     * @param inWire
     */
    void onConsumer(final WireIn inWire);

    /**
     * called when the socket connection is closed
     */
    void onClose();

}
