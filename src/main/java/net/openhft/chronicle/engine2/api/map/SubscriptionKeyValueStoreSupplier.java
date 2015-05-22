package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Interceptor;

import java.util.function.Supplier;

/**
 * Created by peter on 22/05/15.
 */
public interface SubscriptionKeyValueStoreSupplier extends Supplier<SubscriptionKeyValueStore>, Interceptor {
}
