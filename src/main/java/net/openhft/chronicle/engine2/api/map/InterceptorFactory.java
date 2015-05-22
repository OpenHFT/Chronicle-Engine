package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Interceptor;

/**
 * Created by peter on 22/05/15.
 */
public interface InterceptorFactory extends Interceptor {
    <I extends Interceptor> I create(Class<I> iClass);
}
