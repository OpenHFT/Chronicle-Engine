package net.openhft.chronicle.engine;

import net.openhft.chronicle.wire.YamlLogging;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created by daniel on 01/09/2015.
 */
public class ChronicleConfig implements ChronicleConfigMBean {
    private boolean yamlServerLogging;
    private boolean yamlClientLogging;

    @Override
    public void setYamlServerLogging(boolean yamlServerLogging) {
        this.yamlServerLogging = yamlServerLogging;
        YamlLogging.showServerReads = yamlServerLogging;
        YamlLogging.showServerWrites = yamlServerLogging;
    }

    @Override
    public boolean getYamlServerLogging() {
        return yamlServerLogging;
    }

    @Override
    public void setYamlClientLogging(boolean yamlClientLogging) {
        YamlLogging.clientReads = yamlClientLogging;
        YamlLogging.clientWrites = yamlClientLogging;
        this.yamlClientLogging = yamlClientLogging;
    }

    @Override
    public boolean getYamlClientLogging() {
        return yamlClientLogging;
    }

    public static void init(){
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ChronicleConfig mBean = new ChronicleConfig();
        try {
            ObjectName name = new ObjectName("net.openhft.chronicle.engine:type=ChronicleConfig");
            mbs.registerMBean(mBean, name);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }
}
