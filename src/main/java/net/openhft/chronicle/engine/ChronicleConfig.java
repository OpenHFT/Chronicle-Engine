package net.openhft.chronicle.engine;

import net.openhft.chronicle.wire.YamlLogging;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created by daniel on 01/09/2015.
 */
public class ChronicleConfig implements ChronicleConfigMBean {

    @Override
    public void setYamlServerReadLogging(boolean logging) {
        YamlLogging.showServerReads = logging;
    }

    @Override
    public boolean getYamlServerReadLogging() {
        return YamlLogging.showServerReads;
    }

    @Override
    public void setYamlClientReadLogging(boolean logging) {
        YamlLogging.clientReads = logging;
    }

    @Override
    public boolean getYamlClientReadLogging() {
        return YamlLogging.clientReads;
    }

    @Override
    public void setYamlServerWriteLogging(boolean logging) {
        YamlLogging.showServerWrites = logging;
    }

    @Override
    public boolean getYamlServerWriteLogging() {
        return YamlLogging.showServerWrites;
    }

    @Override
    public void setYamlClientWriteLogging(boolean logging) {
        YamlLogging.clientWrites = logging;
    }

    @Override
    public boolean getYamlClientWriteLogging() {
        return YamlLogging.clientWrites;
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
