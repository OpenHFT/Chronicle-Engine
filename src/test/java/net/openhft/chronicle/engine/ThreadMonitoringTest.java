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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static net.openhft.chronicle.core.Jvm.pause;

/**
 * Created by Rob Austin
 */
public class ThreadMonitoringTest {

    Set<Thread> threads;

    public static void checkThreadsShutdown(@NotNull Set<Thread> threads) {
        Thread.interrupted();
        // give them a change to stop if there were killed.
        pause(100);
        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        threadMap.keySet().removeAll(threads);
        threadMap.keySet().removeAll(
                threadMap.keySet().stream()
                        .filter(t -> t.getName()
                                .startsWith("ForkJoinPool.commonPool-worker"))
                        .collect(Collectors.toList()));
        if (threadMap.isEmpty()) {
            return;
        }
        System.out.println("### threads still running after the test ###");
        for (Entry<Thread, StackTraceElement[]> entry : threadMap.entrySet()) {
            StringBuilder sb = new StringBuilder(entry.getKey().toString());
            Jvm.trimStackTrace(sb, entry.getValue());
            System.out.println(sb);
        }

        for (Thread thread : threadMap.keySet()) {
            if (thread.isAlive()) {
                System.out.println("Waiting for " + thread);
                try {
                    thread.join(1000);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                if (thread.isAlive()) {
                    System.out.println("Forcing " + thread + " to die");
                    thread.stop();
                }
            }
        }
    }

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        checkThreadsShutdown(threads);
    }
}
