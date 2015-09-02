package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;
import net.openhft.engine.chronicle.demo.data.EndOfDayShort;

import java.util.Map;

/**
 * Created by daniel on 02/09/2015.
 * A very simple program to demonstrate how to change value.
 */
public class ChangeEndOfDayShort {
    public static void main(String[] args) {
        AssetTree clientAssetTree = new VanillaAssetTree().
                forRemoteAccess("localhost:8088", WireType.BINARY);
        Map<String, EndOfDayShort> ftseMap = clientAssetTree.acquireMap("/stocks/ftse", String.class, EndOfDayShort.class);
        EndOfDayShort eodShort = ftseMap.get("3IN");
        eodShort.closingPrice = 9.99995;
        ftseMap.put("3IN", eodShort);
    }
}
