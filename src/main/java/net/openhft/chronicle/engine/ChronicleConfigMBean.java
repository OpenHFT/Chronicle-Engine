package net.openhft.chronicle.engine;

/**
 * Created by daniel on 01/09/2015.
 */
public interface ChronicleConfigMBean {
    void setYamlServerReadLogging(boolean log);
    boolean getYamlServerReadLogging();
    void setYamlClientReadLogging(boolean log);
    boolean getYamlClientReadLogging();

    void setYamlServerWriteLogging(boolean log);
    boolean getYamlServerWriteLogging();
    void setYamlClientWriteLogging(boolean log);
    boolean getYamlClientWriteLogging();
}
