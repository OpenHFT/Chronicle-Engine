/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
 *  *
 *  *     This program is free software: you can redistribute it and/or modify
 *  *     it under the terms of the GNU Lesser General Public License as published by
 *  *     the Free Software Foundation, either version 3 of the License.
 *  *
 *  *     This program is distributed in the hope that it will be useful,
 *  *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  *     GNU Lesser General Public License for more details.
 *  *
 *  *     You should have received a copy of the GNU Lesser General Public License
 *  *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.time.LocalTime;

/**
 * Created by daniel on 07/09/2015.
 */
public class UserStat implements Marshallable {
    public LocalTime loggedIn;
    public LocalTime recentInteraction;
    public int totalInteractions = 0;

    public LocalTime getLoggedIn() {
        return loggedIn;
    }

    public void setLoggedIn(LocalTime loggedIn) {
        this.loggedIn = loggedIn;
    }

    public LocalTime getRecentInteraction() {
        return recentInteraction;
    }

    public void setRecentInteraction(LocalTime recentInteraction) {
        this.recentInteraction = recentInteraction;
    }

    public int getTotalInteractions() {
        return totalInteractions;
    }

    public void setTotalInteractions(int totalInteractions) {
        this.totalInteractions = totalInteractions;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        wire.read(() -> "loggedIn").time(this, (o, b) -> o.loggedIn = b)
                .read(() -> "recentInteraction").time(this, (o, b) -> o.recentInteraction = b)
                .read(() -> "totalInteractions").int16(this, (o, b) -> o.totalInteractions = b);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "loggedIn").time(loggedIn)
                .write(() -> "recentInteraction").time(recentInteraction)
                .write(() -> "totalInteractions").int16(totalInteractions);
    }

    @Override
    public String toString() {
        return "MonitorCfg{" +
                " loggedIn=" + loggedIn +
                " recentInteraction=" + recentInteraction +
                " totalInteractions=" + totalInteractions +
                '}';
    }
}
