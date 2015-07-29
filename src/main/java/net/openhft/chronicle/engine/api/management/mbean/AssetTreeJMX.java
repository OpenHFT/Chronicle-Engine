package net.openhft.chronicle.engine.api.management.mbean;

import net.openhft.chronicle.engine.api.management.ManagementTools;
import net.openhft.chronicle.engine.map.ObjectKVSSubscription;
import net.openhft.chronicle.engine.map.ObjectKeyValueStore;
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

    public AssetTreeJMX(@NotNull ObjectKeyValueStore view, @NotNull ObjectKVSSubscription objectKVSSubscription, String path, String entries) {
        this.size = view.longSize();
        this.entries = entries;
        this.keyTypeClass = view.keyType();
        this.keyType = keyTypeClass.getName();
        this.valueTypeClass = view.valueType();
        this.valueType = valueTypeClass.getName();
        this.topicSubscriberCount = objectKVSSubscription.topicSubscriberCount();
        this.keySubscriberCount = objectKVSSubscription.keySubscriberCount();
        this.entrySubscriberCount = objectKVSSubscription.entrySubscriberCount();
        this.keyStoreValue = objectKVSSubscription.getClass().getName();
        this.path = path;
    }

    @Override
    public void setSize(long size) {
        this.size = size;
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