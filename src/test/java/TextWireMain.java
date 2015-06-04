import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.wire.TextWire;

import java.io.IOException;

/**
 * Created by andre on 01/05/2015.
 */
public class TextWireMain {
    public static void main(String[] args) throws IOException {

        int noPutsAndGets = 50;
        final int MB = 1 << 20;

        try {
            int port = 8088;
            final ServerEndpoint serverEndpoint = new ServerEndpoint(port, TextWire::new);

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
