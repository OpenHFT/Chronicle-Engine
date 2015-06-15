package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.View;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by peter.lawrey on 15/06/2015.
 */
public class HostIdentifier implements View {
    private final int hostId;
    private final List<Integer> remoteIds = Collections.synchronizedList(new ArrayList<>());

    public HostIdentifier(int hostId) {
        this.hostId = hostId;
    }

    public int hostId() {
        return hostId;
    }

    public List<Integer> remoteIds() {
        return remoteIds;
    }
}
