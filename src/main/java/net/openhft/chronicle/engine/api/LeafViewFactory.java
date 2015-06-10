package net.openhft.chronicle.engine.api;

import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 22/05/15.
 */
public interface LeafViewFactory<I> {
    @NotNull
    I create(RequestContext context, Asset asset) throws AssetNotFoundException;
}
