package net.openhft.chronicle.engine2.api;

import net.openhft.chronicle.engine2.session.LocalSession;

/**
 * Created by peter on 22/05/15.
 */
public interface View {
    default boolean keyedView() {
        return false;
    }

    View forSession(LocalSession session, Asset asset);

    static <I> I forSession(I i, LocalSession session, Asset asset) {
        return i instanceof View ? (I) ((View) i).forSession(session, asset) : i;
    }
}
