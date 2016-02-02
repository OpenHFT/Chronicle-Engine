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
