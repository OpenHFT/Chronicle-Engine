package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.engine.query.VanillaQuery;
import org.jetbrains.annotations.NotNull;

import java.util.stream.Stream;

/**
 * Created by peter.lawrey on 11/07/2015.
 */
public interface Queryable<E> {
    @NotNull
    default Query<E> query() {
        return new VanillaQuery<>(stream());
    }

    Stream<E> stream();
}
