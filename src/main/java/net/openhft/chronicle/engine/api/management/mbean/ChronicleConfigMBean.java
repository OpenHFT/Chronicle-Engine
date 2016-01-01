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
