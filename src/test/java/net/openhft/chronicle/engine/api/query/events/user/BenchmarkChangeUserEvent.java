package net.openhft.chronicle.engine.api.query.events.user;

import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class BenchmarkChangeUserEvent extends CellKey implements UserEvent {

    @NotNull
    private String benchmark;
    @NotNull
    private String userName;

    public BenchmarkChangeUserEvent(@NotNull String cellId,
                                    @NotNull String benchmark,
                                    @NotNull String userName) {
        super(cellId);
        this.benchmark = benchmark;
        this.userName = userName;
    }

    @NotNull
    public String benchmark() {
        return benchmark;
    }

    @NotNull
    public BenchmarkChangeUserEvent benchmark(@NotNull String benchmark) {
        this.benchmark = benchmark;
        return this;
    }

    @NotNull
    @Override
    public String userName() {
        return userName;
    }
}
