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

package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.core.threads.ThreadDump;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.wire.YamlLogging;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Map;
import java.util.Properties;

/**
 * Created by Rob Austin
 */
public class ThreadMonitoringTest {
    static final Properties prop0 = new Properties();

    static {
        prop0.putAll(System.getProperties());
    }

    protected ThreadDump threadDump;
    private Map<ExceptionKey, Integer> exceptions;

    @Before
    public void recordExceptions() {
        exceptions = Jvm.recordExceptions();
    }

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

    @Before
    public void resetProperties() {
        System.getProperties().clear();
        System.getProperties().putAll(prop0);
    }

    @After
    public final void after() {
        preAfter();

        TCPRegistry.reset();
        threadDump.ignore("main/ChronicleMapKeyValueStore Closer");
        threadDump.ignore("tree-1/Heartbeat");
        threadDump.ignore("tree-2/Heartbeat");
        threadDump.ignore("tree-3/Heartbeat");
        threadDump.ignore("tree-1/closer");
        threadDump.ignore("tree-2/closer");
        threadDump.ignore("tree-3/closer");
        threadDump.assertNoNewThreads();
        YamlLogging.setAll(false);
        resetProperties();

        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
        recordExceptions();
    }

    protected void preAfter() {
        if (!exceptions.isEmpty()) {
            Jvm.dumpException(exceptions);
            Jvm.resetExceptionHandlers();
            Assert.fail();
        }
    }
}
