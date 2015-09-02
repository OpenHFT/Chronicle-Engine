package net.openhft.engine.chronicle.demo;

import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.api.tree.AssetTree;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.wire.WireType;

import java.util.Scanner;

/**
 * Created by daniel on 02/09/2015.
 * This demo program allows the user to interactively
 * change values in the /data/map.
 */
public class UpdateMap {
    public static void main(String[] args) {
        AssetTree clientAssetTree = new VanillaAssetTree().
                forRemoteAccess("localhost:8088", WireType.BINARY);
        MapView<String, String> dataMap = clientAssetTree.acquireMap("/data/map", String.class, String.class);
        dataMap.put("Hello", "World");

        Scanner scanner = new Scanner(System.in);
        usage();

        while(true){
            String input = scanner.nextLine();
            String[] parts = input.split(" ");
            if(parts.length<1){
                usage();
                continue;
            }
            if(parts[0].equalsIgnoreCase("size")){
                if(parts.length != 1){
                    usage();
                    continue;
                }
                System.out.println("Size of map:" + dataMap.size());
            }else if(parts[0].equalsIgnoreCase("put")){
                if(parts.length != 3){
                    usage();
                    continue;
                }
                dataMap.put(parts[1], parts[2]);
                System.out.println("Put: key-" + parts[1] + " value-" + parts[2]);
            }else if(parts[0].equalsIgnoreCase("get")){
                if(parts.length != 2){
                    usage();
                    continue;
                }
                System.out.println("Get for key-" + parts[1] + " " + dataMap.get(parts[1]));
            }else if(parts[0].equalsIgnoreCase("remove")){
                if(parts.length != 2){
                    usage();
                    continue;
                }
                dataMap.remove(parts[1]);
                System.out.println("Removed key-" + parts[1] );
            }else{
                usage();
            }
        }
    }

    private static void usage(){
        System.out.println("Usage:");
        System.out.println("Size:");
        System.out.println("Put: key value");
        System.out.println("Get: key");
        System.out.println("Remove: key");
    }
}
