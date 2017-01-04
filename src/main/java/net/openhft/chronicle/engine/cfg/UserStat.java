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

    @NotNull
    @Override
    public String toString() {
        return "MonitorCfg{" +
                " loggedIn=" + loggedIn +
                " recentInteraction=" + recentInteraction +
                " totalInteractions=" + totalInteractions +
                '}';
    }
}
