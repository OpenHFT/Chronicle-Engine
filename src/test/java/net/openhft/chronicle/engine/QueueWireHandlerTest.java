package net.openhft.chronicle.engine;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.SingleChronicleQueue;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static net.openhft.chronicle.engine.QueueWireHandler.Fields.*;

/**
 * Created by daniel on 01/04/15.
 */
public class QueueWireHandlerTest extends ThreadMonitoringTest {
    /**
     * Simple reference test using wire on a SingleChronicleQueue directly
     * @throws IOException
     */
    @Test
    public void testSimple() throws IOException {
        File f = new File("/tmp/qservertest");
        f.delete();
        f.deleteOnExit();
        SingleChronicleQueue q = new SingleChronicleQueue("/tmp/qservertest", 1024);
        ExcerptAppender appender = q.createAppender();
        for (int i = 0; i < 5; i++) {
            appender.writeDocument(wire -> wire.write(() -> "Message").text("Hello"));
            System.out.println(appender.lastWrittenIndex());
        }

        StringBuilder sb = new StringBuilder();

        ExcerptTailer tailer = q.createTailer();
        tailer.readDocument(wire -> wire.read(() -> "Message").text(sb));

        Assert.assertEquals("Hello", sb.toString());
    }

    @Test
    public void testStatelessOneRW() throws IOException {
        File f = new File("/tmp/qservertest");
        f.delete();
        f.deleteOnExit();
        QueueWireHandler q = new QueueWireHandler();

        Wire in = new TextWire(Bytes.elasticByteBuffer());
        Wire out = new TextWire(Bytes.elasticByteBuffer());

        /**
         * Create an appender
         */
        in.writeDocument(true, wireOut -> {
            wireOut.write(csp).text("//localhost/tmp/testQueue#Queue");
            wireOut.write(tid).int64(System.nanoTime());
        });


        in.writeDocument(false, wireOut -> {
            wireOut.writeEventName(createAppender).marshallable(wire -> {
            });
        });

        q.process(in, out);

        /**
         * Read the cid response from the server
         */
        QueueAppender[] qaArr = new QueueAppender[1];

        out.readDocument(wireIn -> {
            long tid1 = wireIn.read(tid).int64();
        }, wireIn -> {
            ValueIn read = wireIn.read(reply);
            qaArr[0] = (QueueAppender) read.typedMarshallable();
        });

        /**
         * Write an object to the queue
         */
        in.writeDocument(true, wireOut -> {
            wireOut.write(cid).int32(qaArr[0].getCid());
            wireOut.write(tid).int64(System.nanoTime());
        });

        StringBuilder sb = new StringBuilder();
        sb.append("dan");
        TestMarshallable tm = new TestMarshallable();
        tm.setName(sb);
        tm.setCount(10);

        in.writeDocument(false, wireOut -> {
            wireOut.writeEventName(submit).typedMarshallable(tm);
        });

        q.process(in, out);


        /**
         * Read the index from the queue
         */
        out.readDocument(wireIn -> {
            long tid1 = wireIn.read(tid).int64();
        }, wireIn -> {
            ValueIn read = wireIn.read(index);
            System.out.println("Index = " + read.int64());
        });
    }
}
