/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.engine.api.query;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by rob on 27/04/2016.
 */
public class IndexedValue<V extends Marshallable> implements Demarshallable, Marshallable {

    private long index;
    private long timePublished;
    private long maxIndex;
    @Nullable
    private V v;
    private transient Object k;

    IndexedValue() {
    }

    @UsedViaReflection
    private IndexedValue(@NotNull WireIn wire) {
        readMarshallable(wire);
    }

    IndexedValue(Object k, V v, long index) {
        this.v = v;
        this.k = k;
        this.index = index;
    }

    IndexedValue(V v, long index) {
        this.v = v;
        this.index = index;
    }

    /**
     * @return the maximum index that is currently available, you can compare this index with the
     * {@code index} to see how many records the currently even is behind.
     */
    public long maxIndex() {
        return maxIndex;
    }

    @NotNull
    public IndexedValue maxIndex(long maxIndex) {
        this.maxIndex = maxIndex;
        return this;
    }

    /**
     * @return the {@code index} in the chronicle queue, of this event
     */
    public long index() {
        return index;
    }

    @NotNull
    public IndexedValue index(long index) {
        this.index = index;
        return this;
    }

    @Nullable
    public V v() {
        return v;
    }

    @NotNull
    public IndexedValue v(V v) {
        this.v = v;
        return this;
    }

    public Object k() {
        return k;
    }

    @NotNull
    public IndexedValue k(Object k) {
        this.k = k;
        return this;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write("index").int64_0x(index);
        wire.write("v").typedMarshallable(v);
        wire.write("timePublished").int64(timePublished);
        wire.write("maxIndex").int64(maxIndex);
    }

    @Override
    public String toString() {
        return Marshallable.$toString(this);
    }

    public long timePublished() {
        return timePublished;
    }

    @NotNull
    public IndexedValue timePublished(long timePublished) {
        this.timePublished = timePublished;
        return this;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        index = wire.read(() -> "index").int64();
        v =  wire.read(() -> "v").typedMarshallable();
        timePublished = wire.read(() -> "timePublished").int64();
        maxIndex = wire.read(() -> "maxIndex").int64();
    }

}

