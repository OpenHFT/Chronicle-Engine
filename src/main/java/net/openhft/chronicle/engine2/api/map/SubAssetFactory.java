package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Asset;
import net.openhft.chronicle.engine2.api.Interceptor;

/**
 * Created by peter on 22/05/15.
 */
public interface SubAssetFactory extends Interceptor {
    SubAsset create(Asset asset, String name, String query);
}
