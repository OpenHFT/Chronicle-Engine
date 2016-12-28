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

package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.network.MarshallableFunction;
import net.openhft.chronicle.network.connection.VanillaWireOutPublisher;
import net.openhft.chronicle.network.connection.WireOutPublisher;
import net.openhft.chronicle.wire.Demarshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.WireType;
import org.jetbrains.annotations.NotNull;

/**
 * @author Rob Austin.
 */
public class VanillaWireOutPublisherFactory implements MarshallableFunction<WireType,
        WireOutPublisher>, Demarshallable {

    private VanillaWireOutPublisherFactory(WireIn wireIn) {
    }

    public VanillaWireOutPublisherFactory() {
    }

    @NotNull
    @Override
    public WireOutPublisher apply(WireType wireType) {
        return new VanillaWireOutPublisher(wireType);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {

    }
}
