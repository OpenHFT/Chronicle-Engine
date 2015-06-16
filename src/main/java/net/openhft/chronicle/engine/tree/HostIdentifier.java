package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.View;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by peter.lawrey on 15/06/2015.
 */
public class HostIdentifier implements View {

    private final byte hostId;
    private final List<InetAddress> remoteAddresses = Collections.synchronizedList(new
            ArrayList<>());

    public HostIdentifier(byte hostId) {
        this.hostId = hostId;
    }

    public byte hostId() {
        return hostId;
    }

    public List<InetAddress> remoteAddresses() {
        return remoteAddresses;
    }
}
