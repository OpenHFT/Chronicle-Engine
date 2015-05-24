package net.openhft.chronicle.engine2.api;

/**
 * Created by peter on 22/05/15.
 */
public interface Factory<I> {
    I create(FactoryContext context);
}
