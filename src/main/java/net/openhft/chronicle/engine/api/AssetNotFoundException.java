package net.openhft.chronicle.engine.api;

/**
 * This asset could not be found or created on demand.
 * <p>
 * Created by peter on 22/05/15.
 */
public class AssetNotFoundException extends IllegalStateException {
    public AssetNotFoundException(String name) {
        super(name);
    }
}
