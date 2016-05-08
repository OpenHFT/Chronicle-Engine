package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.wire.ReadMarshallable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by peter on 08/05/16.
 */
public enum VanillaObjectCacheFactory implements ObjectCacheFactory {
    INSTANCE;

    @Override
    public Function<Class, ReadMarshallable> get() {
        Map<Class<ReadMarshallable>, ReadMarshallable> cache = new LinkedHashMap<>();
        return c -> cache.computeIfAbsent(c, ObjectUtils::newInstance);
    }
}
