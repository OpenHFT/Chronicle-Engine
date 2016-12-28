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

package net.openhft.chronicle.engine.cfg;

import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by peter on 26/08/15.
 */
public class JmxCfg extends AbstractMarshallable implements Installable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxCfg.class);
    @UsedViaReflection
    private boolean enabled;

    @NotNull
    @Override
    public JmxCfg install(String path, @NotNull AssetTree assetTree) {
        if (enabled) {
            LOGGER.info("Enabling JMX for " + assetTree);
            assetTree.enableManagement();
        }
        return this;
    }
}
