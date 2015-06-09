package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.core.util.ThrowingSupplier;

/**
 * Created by peter on 22/05/15.
 */
public interface ViewFactory<I> {
    I create(RequestContext context, Asset asset, ThrowingSupplier<Assetted, AssetNotFoundException> underlying) throws AssetNotFoundException;
}
