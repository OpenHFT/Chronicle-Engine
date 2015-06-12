package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.server.WireType;

import java.io.IOException;

/**
 * Created by andre on 01/05/2015.
 */
public class TextWireMain {
    public static void main(String[] args) throws IOException, InterruptedException {

        WireType.wire = WireType.TEXT;

        // the default is BinaryWire
        BinaryWireMain.main(args);
    }
}
