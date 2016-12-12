package net.openhft.chronicle.engine;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * @author Rob Austin.
 */
public class X {
    public static void main(String[] args) {
        SingleChronicleQueue build =   SingleChronicleQueueBuilder.binary
                ("/Users/robaustin/git-projects/Chronicle-Engine/queue/shares/APPL")
                .build();


        System.out.println(build.dump());
    }

}
