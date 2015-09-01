package net.openhft.chronicle.engine;

/**
 * Created by daniel on 01/09/2015.
 */
public interface ChronicleConfigMBean {
    void setYamlServerLogging(boolean log);
    boolean getYamlServerLogging();
    void setYamlClientLogging(boolean log);
    boolean getYamlClientLogging();
}
