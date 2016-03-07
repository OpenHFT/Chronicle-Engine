package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.tree.Asset;
import net.openhft.chronicle.engine.tree.HostIdentifier;
import net.openhft.chronicle.network.VanillaNetworkContext;
import org.jetbrains.annotations.NotNull;


/**
 * @author Rob Austin.
 */
public class EngineWireNetworkContext<T extends EngineWireNetworkContext> extends VanillaNetworkContext<T> {

    private Asset rootAsset;

    @NotNull
    public Asset rootAsset() {
        return rootAsset;
    }


    @Override
    public String toString() {

        byte localId = -1;
        if (rootAsset == null) {
            return "";
        }

        final Asset root = rootAsset;
        final HostIdentifier hostIdentifier = root.getView(HostIdentifier.class);

        if (hostIdentifier != null)
            localId = hostIdentifier.hostId();

        return "EngineWireNetworkContext{" +
                "localId=" + localId +
                "rootAsset=" + root +
                '}';
    }

    public EngineWireNetworkContext<T> rootAsset(Asset asset) {
        this.rootAsset = asset.root();
        return this;
    }
}

