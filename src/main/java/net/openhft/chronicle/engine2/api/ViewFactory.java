package net.openhft.chronicle.engine2.api;

import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public interface ViewFactory<I> {
    I create(RequestContext context, Asset asset, Supplier<Assetted> underlying);
}
