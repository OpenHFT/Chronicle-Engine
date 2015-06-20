package net.openhft.chronicle.engine.api.management.mbean;

/**
 * Created by pct25 on 6/16/2015.
 */
public interface AssetTreeJMXMBean {

    public void setSize(long size);

    /*public String getElements();*/
    public long getSize();
    public String getKeyType();
    public String getValueType();
    public int getTopicSubscriberCount();
    public int getKeySubscriberCount();
    public int getEntrySubscriberCount();
    public String getPath();
    public String getKeyStoreValue();

    public void notifyMe(String key, String value);
}
