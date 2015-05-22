package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Assetted<U> {
    void asset(Asset asset);

    Asset asset();

    void underlying(U underlying);

    U underlying();
}
