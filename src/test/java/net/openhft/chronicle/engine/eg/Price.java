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

package net.openhft.chronicle.engine.eg;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Created by peter on 17/08/15.
 */
public class Price implements Marshallable {
    @Nullable
    String instrument;
    double bidPrice, bidQuantity;
    double askPrice, askQuantity;

    public Price(String instrument, double bidPrice, double bidQuantity, double askPrice, double askQuantity) {
        this.instrument = instrument;
        this.bidPrice = bidPrice;
        this.bidQuantity = bidQuantity;
        this.askPrice = askPrice;
        this.askQuantity = askQuantity;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IllegalStateException {
        instrument = wire.read(() -> "instrument").text();
        bidPrice = wire.read(() -> "bidPrice").float64();
        bidQuantity = wire.read(() -> "bidQuantity").float64();
        askPrice = wire.read(() -> "askPrice").float64();
        askQuantity = wire.read(() -> "askQuantity").float64();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "instrument").text(instrument);
        wire.write(() -> "bidPrice").float64(bidPrice);
        wire.write(() -> "bidQuantity").float64(bidQuantity);
        wire.write(() -> "askPrice").float64(askPrice);
        wire.write(() -> "askQuantity").float64(askQuantity);
    }

    @NotNull
    @Override
    public String toString() {
        return "Price{" +
                "instrument='" + instrument + '\'' +
                ", bidPrice=" + bidPrice +
                ", bidQuantity=" + bidQuantity +
                ", askPrice=" + askPrice +
                ", askQuantity=" + askQuantity +
                '}';
    }
}
