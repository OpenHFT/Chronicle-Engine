import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.server.ServerEndpoint;

import java.io.IOException;

/**
 * Created by andre on 01/05/2015.
 */
public class TextWireMain {
    public static void main(String[] args) {
        ChronicleEngine chronicleEngine = new ChronicleEngine();
        try {
            int port = 8088;
            final ServerEndpoint serverEndpoint = new ServerEndpoint(port, (byte) 1, chronicleEngine);

            System.out.println("Server port seems to be " + serverEndpoint.getPort());
            while (true) {
                Thread.sleep(1000 * 10);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
