package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface AssetFactory extends Interceptor {
    Asset create(Asset parent, String name, Assetted item);
}
