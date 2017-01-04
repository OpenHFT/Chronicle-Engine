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

package net.openhft.chronicle.engine.query;

/**
 * @author Rob Austin.
 */

import net.openhft.chronicle.core.annotation.NotNull;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.Nullable;

public class Operation implements Marshallable {

    private OperationType type;
    @Nullable
    private Object wrapped;

    public Operation() {
    }

    public Operation(@NotNull final OperationType type,
                     @NotNull final Object wrapped) {
        this.type = type;
        this.wrapped = wrapped;
    }

    public OperationType op() {
        return type;
    }

    @Nullable
    public <T> T wrapped() {
        return (T) wrapped;
    }

    @Override
    public void readMarshallable(WireIn wireIn) throws IllegalStateException {
        this.type = OperationType.valueOf(wireIn.read(() -> "type").text());
        this.wrapped = wireIn.read(() -> "wrapped").object(Object.class);
    }

    @Override
    public void writeMarshallable(@org.jetbrains.annotations.NotNull WireOut wireOut) {
        wireOut.write(() -> "type").text(type.toString());
        wireOut.write(() -> "wrapped").object(wrapped);
    }

    @org.jetbrains.annotations.NotNull
    @Override
    public String toString() {
        return "Operation{" +
                "type=" + type +
                ", wrapped=" + wrapped +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Operation)) return false;

        @org.jetbrains.annotations.NotNull Operation operation = (Operation) o;

        if (type != operation.type) return false;
        return !(wrapped != null ? !wrapped.equals(operation.wrapped) : operation.wrapped != null);

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (wrapped != null ? wrapped.hashCode() : 0);
        return result;
    }

    public enum OperationType {
        MAP, FILTER, PROJECT, FLAT_MAP
    }
}