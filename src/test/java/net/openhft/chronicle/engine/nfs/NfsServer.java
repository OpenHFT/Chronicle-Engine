package net.openhft.chronicle.engine.nfs;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Rob Austin.
 */
public class NfsServer {


    @Test
    @Ignore("nfs service test - not required for CI")
    public void test() throws Exception {

        ChronicleNfsServer.start();

        //  currently  test using a linux server, to run type
        //
        // to mount :
        // sudo mount -t nfs localhost:/ /mnt
        //
        // the following example creates an entry containg key=hello value=world in the asset
        // called /temp
        //
        // $cd /mnt
        // $mkdir temp
        // cd temp
        // echo hello > world
        //
        // to unmount :
        // $sudo umount /mnt

    }

}
