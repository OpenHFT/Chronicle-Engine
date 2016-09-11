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
public class SubscriptionStat implements Marshallable {
    public LocalTime firstSubscribed;
    public LocalTime recentlySubscribed;
    public int activeSubscriptions = 0;
    public int totalSubscriptions = 0;

    public LocalTime getFirstSubscribed() {
        return firstSubscribed;
    }

    public void setFirstSubscribed(LocalTime firstSubscribed) {
        this.firstSubscribed = firstSubscribed;
    }

    public LocalTime getRecentlySubscribed() {
        return recentlySubscribed;
    }

    public void setRecentlySubscribed(LocalTime recentlySubscribed) {
        this.recentlySubscribed = recentlySubscribed;
    }

    public int getActiveSubscriptions() {
        return activeSubscriptions;
    }

    public void setActiveSubscriptions(int activeSubscriptions) {
        this.activeSubscriptions = activeSubscriptions;
    }

    public int getTotalSubscriptions() {
        return totalSubscriptions;
    }

    public void setTotalSubscriptions(int totalSubscriptions) {
        this.totalSubscriptions = totalSubscriptions;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wire) throws IORuntimeException {
        wire.read(() -> "firstSubscribed").time(this, (o, b) -> o.firstSubscribed = b)
                .read(() -> "recentlySubscribed").time(this, (o, b) -> o.recentlySubscribed = b)
                .read(() -> "activeSubscriptions").int16(this, (o, b) -> o.activeSubscriptions = b)
                .read(() -> "totalSubscriptions").int16(this, (o, b) -> o.totalSubscriptions = b);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wire) {
        wire.write(() -> "firstSubscribed").time(firstSubscribed)
                .write(() -> "recentlySubscribed").time(recentlySubscribed)
                .write(() -> "activeSubscriptions").int16(activeSubscriptions)
                .write(() -> "totalSubscriptions").int16(totalSubscriptions);
    }

    @Override
    public String toString() {
        return "MonitorCfg{" +
                " firstSubscribed=" + firstSubscribed +
                " recentlySubscribed=" + recentlySubscribed +
                " activeSubscriptions=" + activeSubscriptions +
                " totalSubscriptions=" + totalSubscriptions +
                '}';
    }
}
