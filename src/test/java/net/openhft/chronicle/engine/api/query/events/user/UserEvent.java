package net.openhft.chronicle.engine.api.query.events.user;

import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public interface UserEvent extends Marshallable {

    /**
     * @return the desk that made the change
     */
    @NotNull
    String userName();

}


