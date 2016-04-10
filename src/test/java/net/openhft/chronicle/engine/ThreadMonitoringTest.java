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
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Rob Austin
 */
public class ThreadMonitoringTest {

    protected static final AtomicReference<Throwable> t = new AtomicReference<>();
    protected ThreadDump threadDump;

    @Before
    public void turnOffYamlLogging() {
        YamlLogging.setAll(false);
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @Before
    public void clearExistingConnections() {
        TCPRegistry.reset();
    }

    @After
    public final void after() {
        preAfter();

        final Throwable th = t.getAndSet(null);
        if (th != null) throw Jvm.rethrow(th);

        TCPRegistry.assertAllServersStopped();
        threadDump.ignore("main/ChronicleMapKeyValueStore Closer");
        threadDump.ignore("tree-1/Heartbeat");
        threadDump.ignore("tree-2/Heartbeat");
        threadDump.ignore("tree-3/Heartbeat");
        threadDump.assertNoNewThreads();
        YamlLogging.setAll(false);
    }

    protected void preAfter() {

    }

    @After
    public final void checkThreadDump() {

    }

    @After
    public final void afterMethod() {

    }

}
