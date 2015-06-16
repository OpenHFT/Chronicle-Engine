package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.engine.api.tree.ChangeEvent;
import net.openhft.chronicle.wire.WireKey;

/**
 * Created by peter on 11/06/15.
 */
public interface TopologicalEvent extends ChangeEvent {
    String name();

    boolean added();

    default String fullName() {
        String parent = assetName();
        return parent == null ? "/"
                : (parent.isEmpty() || parent.equals("/")) ? "/" + name()
                : parent + "/" + name();
    }

    enum TopologicalFields implements WireKey {
        assetName, name
    }
}
