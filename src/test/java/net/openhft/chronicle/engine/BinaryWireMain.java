package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;

import java.io.IOException;

/**
 * Created by andre on 01/05/2015.
 */
public class BinaryWireMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 8088;
        boolean daemon = false;
        final ServerEndpoint serverEndpoint = new ServerEndpoint(port, daemon, new VanillaAssetTree().forTesting());

        System.out.println("Server port seems to be " + serverEndpoint.getPort());
    }
}
