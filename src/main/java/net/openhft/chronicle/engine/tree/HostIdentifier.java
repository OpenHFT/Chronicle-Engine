package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.View;

/**
 * Created by peter.lawrey on 15/06/2015.
 */
public class HostIdentifier implements View {
    private final byte hostId;

    public HostIdentifier(byte hostId) {
        this.hostId = hostId;
    }

    public byte hostId() {
        return hostId;
    }
}
