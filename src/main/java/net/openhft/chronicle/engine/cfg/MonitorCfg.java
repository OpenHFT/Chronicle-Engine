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

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAsset;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by peter on 26/08/15.
 */
public class MonitorCfg extends AbstractMarshallable implements Installable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitorCfg.class);
    private boolean subscriptionMonitoringEnabled;
    private boolean userMonitoringEnabled;

    @Override
    public MonitorCfg install(String path, AssetTree assetTree) throws IOException, URISyntaxException {
        ((VanillaAsset) assetTree.acquireAsset("/proc")).configMapServer();
        if (subscriptionMonitoringEnabled) {
            LOGGER.info("Enabling Subscription Monitoring for " + assetTree);
            assetTree.acquireMap("/proc/subscriptions", String.class, SubscriptionStat.class);
        }
        if (userMonitoringEnabled) {
            LOGGER.info("Enabling User Monitoring for " + assetTree);
            assetTree.acquireMap("/proc/users", String.class, UserStat.class);
        }
        return this;
    }
}
