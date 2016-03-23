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