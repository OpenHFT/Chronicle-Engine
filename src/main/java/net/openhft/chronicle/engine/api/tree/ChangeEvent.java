package net.openhft.chronicle.engine.api.tree;

import net.openhft.chronicle.wire.Marshallable;

/**
 * Created by peter on 11/06/15.
 */
public interface ChangeEvent extends Marshallable {
    String assetName();
}
