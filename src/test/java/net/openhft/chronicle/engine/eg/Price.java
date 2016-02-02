/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine.eg;

import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Created by peter on 17/08/15.
 */
public class Price implements Marshallable {
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
