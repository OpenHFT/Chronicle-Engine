package net.openhft.chronicle.engine.api.tree;

import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 22/05/15.
 */
@FunctionalInterface
public interface WrappingViewFactory<I, U> {
    @NotNull
    I create(RequestContext context, Asset asset, U underlying) throws AssetNotFoundException;
}
