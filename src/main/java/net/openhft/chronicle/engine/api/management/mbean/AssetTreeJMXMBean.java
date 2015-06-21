package net.openhft.chronicle.engine.api.management.mbean;

/**
 * Created by pct25 on 6/16/2015.
 */
public interface AssetTreeJMXMBean {

    void setSize(long size);

    /*public String getElements();*/
    long getSize();

    String getKeyType();

    String getValueType();

    int getTopicSubscriberCount();

    int getKeySubscriberCount();

    int getEntrySubscriberCount();

    String getPath();

    String getKeyStoreValue();

    void notifyMe(String key, String value);
}
