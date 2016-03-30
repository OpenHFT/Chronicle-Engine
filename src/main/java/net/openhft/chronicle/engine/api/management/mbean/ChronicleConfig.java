/*
 *
 *  *     Copyright (C) 2016  higherfrequencytrading.com
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
            throw new RuntimeException(e);
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
