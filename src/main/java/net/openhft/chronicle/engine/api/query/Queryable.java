package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.engine.query.VanillaQuery;

import java.util.stream.Stream;

/**
 * Created by peter.lawrey on 11/07/2015.
 */
public interface Queryable<E> {
    default Query<E> query() {
        return new VanillaQuery<>(stream());
    }

    Stream<E> stream();
}
