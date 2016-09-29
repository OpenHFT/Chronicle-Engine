package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.core.threads.EventHandler;
import net.openhft.chronicle.core.threads.InvalidEventHandlerException;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.network.NetworkStats;
import net.openhft.chronicle.network.WireNetworkStats;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.RollingChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.openhft.chronicle.wire.Wires.project;

/**
 * populates a map containing a exponential moving average
 *
 * @author Rob Austin.
 */
public class NetworkStatsSummary implements EventHandler {
    private final long index;
    private double alpha = 1.0 / 60.0;
    private final RollingChronicleQueue rollingChronicleQueue;

    @NotNull
    private final MapView<String, Stats> latestStatsPerClientId;

    @NotNull
    private ExcerptTailer tailer = null;

    public NetworkStatsSummary(@NotNull ChronicleQueueView qv, @NotNull MapView<String, Stats> latestStatsPerClientId) {
        rollingChronicleQueue = qv.chronicleQueue();
        this.latestStatsPerClientId = latestStatsPerClientId;
        final Collection<Stats> values = this.latestStatsPerClientId.values();
        final AtomicLong index = new AtomicLong();
        values.forEach(v -> index.set(Math.max(index.get(), v.index)));
        this.index = index.get();

    }

    private final NetworkStats ns = new WireNetworkStats();

    @Override
    public boolean action() throws InvalidEventHandlerException, InterruptedException {
        try {
            if (tailer == null) {
                tailer = rollingChronicleQueue.createTailer();
                if (index > 0)
                    tailer.moveToIndex(index);
                tailer.readingDocument(false).close();
            }
            try (DocumentContext documentContext = tailer.readingDocument(false)) {
                if (!documentContext.isPresent())
                    return false;
                StringBuilder sb = Wires.acquireStringBuilder();
                ValueIn valueIn = documentContext.wire().read(sb);
                if ("NetworkStats".contentEquals(sb)) {
                    valueIn.marshallable(ns);
                    final String userId = ns.userId();

                    if (userId != null && !userId.isEmpty()) {
                        updateMap(ns, documentContext.index());
                    }

                }
            }

            return true;
        } catch (Throwable t) {
            t.printStackTrace();
            return true;
        }
    }

    private Stats stats0 = new Stats();

    private void updateMap(@NotNull NetworkStats ns, final long index) {

        final String key = ns.userId();
        final Stats stats = latestStatsPerClientId.getUsing(key, stats0);
        if (stats == null) {
            Stats value = project(Stats.class, ns);
            value.writeEma = value.writeBps();
            value.readEma = value.readBps();
            value.index = index;
            latestStatsPerClientId.put(key, value);
            return;
        }

        double lastWriteEma = stats.writeEma;
        double lastReadEma = stats.readEma;

        if (equalsSecond(stats.timestamp(), ns.timestamp())) {
            long lastWriteBps = stats.writeBps();
            long lastReadBps = stats.readBps();
            Wires.copyTo(ns, stats);
            stats.writeBps(stats.writeBps() + lastWriteBps);
            stats.readBps(stats.readBps() + lastReadBps);
        } else
            Wires.copyTo(ns, stats);

        stats.writeEma((stats.writeBps() * (1 - alpha)) + (lastWriteEma * alpha));
        stats.readEma((stats.readBps() * (1 - alpha)) + (lastReadEma * alpha));
        latestStatsPerClientId.put(key, stats);
    }

    /**
     * @return true if they are in the same seconds
     */
    private boolean equalsSecond(long t1, long t2) {
        return TimeUnit.MILLISECONDS.toSeconds(t1) == TimeUnit.MILLISECONDS.toSeconds(t2);
    }

    public static class Stats extends WireNetworkStats {
        double writeEma;
        double readEma;
        long index;

        Stats writeEma(double writeEma) {
            this.writeEma = writeEma;
            return this;
        }

        Stats readEma(double readEma) {
            this.readEma = readEma;
            return this;
        }
    }

}