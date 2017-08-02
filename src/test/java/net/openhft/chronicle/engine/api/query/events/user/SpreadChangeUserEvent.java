package net.openhft.chronicle.engine.api.query.events.user;

import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class SpreadChangeUserEvent extends CellKey implements UserEvent {
    double value;
    @NotNull
    String userName;

    public SpreadChangeUserEvent(@NotNull String cellId, double value, @NotNull String userName) {
        super(cellId);
        this.value = value;
        this.userName = userName;
    }

    public double value() {
        return value;
    }

    @Override
    @NotNull
    public String userName() {
        return userName;
    }

}
