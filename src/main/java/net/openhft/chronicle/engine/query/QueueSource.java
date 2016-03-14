package net.openhft.chronicle.engine.query;

import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * @author Rob Austin.
 */
public class QueueSource {

    private final Function<String, Integer> sourceB;

    public QueueSource(Function<String, Integer> queueSource) {
        this.sourceB = queueSource;
    }

    public Integer sourceHostId(@NotNull String uri) {
        return sourceB.apply(uri);
    }

}
