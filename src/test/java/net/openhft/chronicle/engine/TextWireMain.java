package net.openhft.chronicle.engine;

import net.openhft.chronicle.engine.api.WireType;
import net.openhft.chronicle.wire.TextWire;

import java.io.IOException;

/**
 * Created by andre on 01/05/2015.
 */
public class TextWireMain {
    public static void main(String[] args) throws IOException, InterruptedException {

        WireType.wire = TextWire::new;

        // the default is BinaryWire
        BinaryWireMain.main(args);
    }
}
