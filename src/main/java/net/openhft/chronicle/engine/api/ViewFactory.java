package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.core.util.ThrowingSupplier;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 22/05/15.
 */
public interface ViewFactory<I> {
    @NotNull
    I create(RequestContext context, Asset asset, ThrowingSupplier<Assetted, AssetNotFoundException> underlying) throws AssetNotFoundException;
}
