package net.openhft.chronicle.engine.fs;

/**
 * @author Rob Austin.
 */
public class ClusterStarter {

    private String[] connections;

    public void start(String[] connections) {
        this.connections = connections;
    }
}
