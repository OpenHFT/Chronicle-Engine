/*
 *
 *  *     Copyright (C) ${YEAR}  higherfrequencytrading.com
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

/**
 * Created by daniel on 01/09/2015.
 */
public interface ChronicleConfigMBean {
    boolean getYamlServerReadLogging();

    void setYamlServerReadLogging(boolean log);

    boolean getYamlClientReadLogging();

    void setYamlClientReadLogging(boolean log);

    boolean getYamlServerWriteLogging();

    void setYamlServerWriteLogging(boolean log);

    boolean getYamlClientWriteLogging();

    void setYamlClientWriteLogging(boolean log);

    boolean getShowHeartBeats();

    void setShowHeartBeats(boolean log);
}
