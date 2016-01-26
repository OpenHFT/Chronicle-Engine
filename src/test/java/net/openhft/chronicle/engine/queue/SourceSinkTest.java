package net.openhft.chronicle.engine.queue;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;

import static net.openhft.chronicle.bytes.Bytes.wrapForRead;

/**
 * @author Rob Austin.
 */
public class SourceSinkTest {

    @Test
    public void test() throws IOException {

        ChronicleQueueBuilder builder = new SingleChronicleQueueBuilder(Files.createTempDirectory
                ("chronicle" + "-" + System.nanoTime()).toFile());


        final ChronicleQueue chronicleQueue = builder.build();

        final ExcerptAppender appender = chronicleQueue.createAppender();
        appender.writeBytes(wrapForRead("hello world".getBytes()));


      //  remoteQueue - source/sink
        //
          //      publishSoucre
            //            subSink




    }


}
