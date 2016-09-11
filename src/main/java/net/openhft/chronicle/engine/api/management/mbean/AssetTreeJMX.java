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

import net.openhft.chronicle.engine.api.management.ManagementTools;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
import net.openhft.chronicle.engine.map.ObjectSubscription;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pct25 on 6/16/2015.
 */
public class AssetTreeJMX implements AssetTreeJMXMBean {

    private static final Logger LOG = LoggerFactory.getLogger(ManagementTools.class);

    private long size;
    private String entries;
    private String keyType;
    private Class keyTypeClass;
    private String valueType;
    private Class valueTypeClass;
    private int topicSubscriberCount;
    private int keySubscriberCount;
    private int entrySubscriberCount;
    private String keyStoreValue;
    private String path;

    public AssetTreeJMX() {

    }

    public AssetTreeJMX(@NotNull ObjectKeyValueStore view, @NotNull ObjectSubscription objectSubscription, String path, String entries) {
        this.size = view.longSize();
        this.entries = entries;
        this.keyTypeClass = view.keyType();
        this.keyType = keyTypeClass.getName();
        this.valueTypeClass = view.valueType();
        this.valueType = valueTypeClass.getName();
        this.topicSubscriberCount = objectSubscription.topicSubscriberCount();
        this.keySubscriberCount = objectSubscription.keySubscriberCount();
        this.entrySubscriberCount = objectSubscription.entrySubscriberCount();
        this.keyStoreValue = objectSubscription.getClass().getName();
        this.path = path;
    }

    @Override
    public String getEntries() {
        return entries;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String getKeyType() {
        return keyType;
    }

    @Override
    public String getValueType() {
        return valueType;
    }

    @Override
    public int getTopicSubscriberCount() {
        return topicSubscriberCount;
    }

    @Override
    public int getKeySubscriberCount() {
        return keySubscriberCount;
    }

    @Override
    public int getEntrySubscriberCount() {
        return entrySubscriberCount;
    }

    @Override
    public String getKeyStoreValue() {
        return keyStoreValue;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void notifyMe(String key, String value) {
        LOG.info("Added Key = " + keyTypeClass.cast(key));
        LOG.info("Added Value = " + valueTypeClass.cast(value));
        LOG.info("changed size: " + size);
    }
}