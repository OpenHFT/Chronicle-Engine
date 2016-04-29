package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;

/**
 * Created by rob on 27/04/2016.
 */
public class IndexedValue<V extends Marshallable> implements Demarshallable, Marshallable {

    private long index;
    private V v;
    private transient Object k;

    @UsedViaReflection
    private IndexedValue(@NotNull WireIn wire) {
        readMarshallable(wire);
    }

    IndexedValue(Object k, V v, long index) {
        this.v = v;
        this.k = k;
        this.index = index;
    }


    IndexedValue() {

    }

    IndexedValue(V v, long index) {
        this.v = v;
        this.index = index;
    }


    public long index() {
        return index;
    }

    public IndexedValue index(long index) {
        this.index = index;
        return this;
    }

    public V v() {
        return v;
    }

    public IndexedValue v(V v) {
        this.v = v;
        return this;
    }

    public Object k() {
        return k;
    }

    public IndexedValue k(Object k) {
        this.k = k;
        return this;
    }

    @Override
    public String toString() {
        return Marshallable.$toString(this);
    }
}
