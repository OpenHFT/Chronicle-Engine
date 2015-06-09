package net.openhft.chronicle.engine;

import net.openhft.lang.Jvm;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Rob Austin
 */
public class ThreadMonitoringTest {

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        checkThreadsShutdown(threads);
    }

    public static void checkThreadsShutdown(@NotNull Set<Thread> threads) {
        // give them a change to stop if there were killed.
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<Thread, StackTraceElement[]> threadMap = Thread.getAllStackTraces();
        threadMap.keySet().removeAll(threads);
        threadMap.keySet().removeAll(
                threadMap.keySet().stream()
                        .filter(t -> t.getName()
                                .startsWith("ForkJoinPool.commonPool-worker"))
                        .collect(Collectors.toList()));
        if (!threadMap.isEmpty()) {
            System.out.println("### threads still running after the test ###");
            for (Map.Entry<Thread, StackTraceElement[]> entry : threadMap.entrySet()) {
                StringBuilder sb = new StringBuilder(entry.getKey().toString());
                Jvm.trimStackTrace(sb, entry.getValue());
                System.out.println(sb);
            }
            try {
                for (Thread thread : threadMap.keySet()) {
                    if (thread.isAlive()) {
                        System.out.println("Waiting for " + thread);
                        thread.join(1000);
                        if (thread.isAlive()) {
                            System.out.println("Forcing " + thread + " to die");
                            thread.stop();
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
