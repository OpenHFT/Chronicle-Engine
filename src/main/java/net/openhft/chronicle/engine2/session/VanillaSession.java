package net.openhft.chronicle.engine2.session;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.AssetNotFoundException;
import net.openhft.chronicle.engine2.api.Assetted;
import net.openhft.chronicle.engine2.api.Session;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by peter on 22/05/15.
 */
public class VanillaSession implements Session {
    final VanillaAsset root = new VanillaAsset(null, "", null);

    @NotNull
    @Override
    public Asset acquireAsset(String name) throws AssetNotFoundException {
        return name.isEmpty() || name.equals("/") ? root : root.acquireChild(name);
    }

    @Nullable
    @Override
    public Asset getAsset(String name) {
        return name.isEmpty() || name.equals("/") ? root : root.getChild(name);
    }

    @Override
    public Asset add(String name, Assetted resource) {
        return root.add(name, resource);
    }

    @Override
    public void close() {
        root.close();
    }
}
