package net.openhft.chronicle.engine.api.management.mbean;

/**
 * Created by pct25 on 6/16/2015.
 */
public interface AssetTreeJMXMBean {

    String getEntries();

    long getSize();

    void setSize(long size);

    String getKeyType();

    String getValueType();

    int getTopicSubscriberCount();

    int getKeySubscriberCount();

    int getEntrySubscriberCount();

    String getPath();

    String getKeyStoreValue();

    void notifyMe(String key, String value);
}
