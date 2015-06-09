package net.openhft.chronicle.engine.api;

import net.openhft.chronicle.wire.WireKey;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;

/**
 * Created by peter on 22/05/15.
 */
public interface Invocation {
    @NotNull
    String methodName();

    int parameters();

    @NotNull
    WireKey parameter1();

    @NotNull
    WireKey parameter2();

    @NotNull
    WireKey parameter3();

    @NotNull
    WireKey parameter4();

    @NotNull
    Object value1();

    @NotNull
    Object value2();

    @NotNull
    Object value3();

    @NotNull
    Object value4();

    @NotNull
    Method method();

    @NotNull
    Object[] args();
}
