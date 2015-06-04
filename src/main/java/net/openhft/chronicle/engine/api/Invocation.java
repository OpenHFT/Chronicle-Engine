package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.wire.WireKey;

import java.lang.reflect.Method;

/**
 * Created by peter on 22/05/15.
 */
public interface Invocation {
    String methodName();

    int parameters();

    WireKey parameter1();

    WireKey parameter2();

    WireKey parameter3();

    WireKey parameter4();

    Object value1();

    Object value2();

    Object value3();

    Object value4();

    Method method();

    Object[] args();
}
