import net.openhft.chronicle.engine.client.internal.ChronicleEngine;
import net.openhft.chronicle.engine.server.ServerEndpoint;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.TextWire;

import java.io.IOException;

/**
 * Created by andre on 01/05/2015.
 */
public class TextWireMain {
    public static void main(String[] args) throws IOException {

        int noPutsAndGets = 50;
        final int MB = 1 << 20;

        ChronicleEngine chronicleEngine = new ChronicleEngine();
        int valueLength = 2 * MB;
        chronicleEngine.setMap("test", ChronicleMapBuilder
                .of(String.class, CharSequence.class)
                .entries(noPutsAndGets)
                .averageKeySize(16)
                .actualChunkSize(valueLength + 16)
                .putReturnsNull(true)
                .create());

        try {
            int port = 8088;
            final ServerEndpoint serverEndpoint = new ServerEndpoint(port, (byte) 1, TextWire::new);

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
