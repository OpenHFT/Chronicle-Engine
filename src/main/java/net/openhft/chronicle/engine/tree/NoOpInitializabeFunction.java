package net.openhft.chronicle.engine.tree;

import net.openhft.chronicle.wire.WireIn;

/**
 * @author Rob Austin.
 */
public class NoOpInitializabeFunction implements InitializabeFunction {

    private NoOpInitializabeFunction(WireIn w) {

    }

    public NoOpInitializabeFunction() {
    }

    @Override
    public Object apply(Object o) {
        return o;
    }
}
