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

package net.openhft.chronicle.engine.api.management.mbean;

import net.openhft.chronicle.wire.YamlLogging;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created by daniel on 01/09/2015.
 */
public class ChronicleConfig implements ChronicleConfigMBean {

    public static void init() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ChronicleConfig mBean = new ChronicleConfig();
        try {
            ObjectName name = new ObjectName("net.openhft.chronicle.engine:type=ChronicleConfig");
            mbs.registerMBean(mBean, name);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean getYamlServerReadLogging() {
        return YamlLogging.showServerReads();
    }

    @Override
    public void setYamlServerReadLogging(boolean logging) {
        YamlLogging.showServerReads(logging);
    }

    @Override
    public boolean getYamlClientReadLogging() {
        return YamlLogging.showClientReads();
    }

    @Override
    public void setYamlClientReadLogging(boolean logging) {
        YamlLogging.showClientReads(logging);
    }

    @Override
    public boolean getYamlServerWriteLogging() {
        return YamlLogging.showServerWrites();
    }

    @Override
    public void setYamlServerWriteLogging(boolean logging) {
        YamlLogging.showServerWrites(logging);
    }

    @Override
    public boolean getYamlClientWriteLogging() {
        return YamlLogging.showClientWrites();
    }

    @Override
    public void setYamlClientWriteLogging(boolean logging) {
        YamlLogging.showClientWrites(logging);
    }

    @Override
    public boolean getShowHeartBeats() {
        return YamlLogging.showHeartBeats();
    }

    @Override
    public void setShowHeartBeats(boolean log) {
        YamlLogging.showHeartBeats(log);
    }
}
