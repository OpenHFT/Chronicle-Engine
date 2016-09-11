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
