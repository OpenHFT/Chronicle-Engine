package net.openhft.chronicle.engine;

import net.openhft.chronicle.network.WireHandler;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.SingleChronicleQueue;
import net.openhft.chronicle.wire.*;
import util.ColorOut;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static net.openhft.chronicle.engine.QueueWireHandler.Fields.*;

/**
 * Created by daniel on 01/04/15.
 */
public class QueueWireHandler implements WireHandler {
    private ConcurrentHashMap<String, Integer> cspToCid = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SingleChronicleQueue> fileNameToChronicle = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, QueueAppender> cidToQueueAppender = new ConcurrentHashMap<>();
    private AtomicInteger cidCounter = new AtomicInteger();

    public QueueWireHandler() {
    }

    @Override
    public void process(Wire in, Wire out){
        in.flip();
        out.clear();
        System.out.println("Received by server:");

        System.out.println(Wires.fromSizePrefixedBlobs(in.bytes()));
        StringBuilder sbCSP = new StringBuilder();
        int[] clientCID = new int[1];

        Consumer<WireIn> metaDataConsumer = wireIn -> {
            try {
                clientCID[0] = wireIn.read(cid).int32();
            }catch(UnsupportedOperationException e){
                wireIn.read(csp).text(sbCSP);
                clientCID[0] = cspToCid.computeIfAbsent(sbCSP.toString(),
                        s -> cidCounter.incrementAndGet());
            }

            long clientTid = wireIn.read(tid).int64();

            out.writeDocument(true, wireOut -> {
                wireOut.write(tid).int64(clientTid);
            });
        };

        Consumer<WireIn> dataConsumer = wireIn -> {
            StringBuilder sb = Wires.acquireStringBuilder();
            ValueIn value = wireIn.readEventName(sb);

            if (createAppender.contentEquals(sb)){
                try {
                    //Get the name from the csp
                    String[] parts = sbCSP.toString().split("/");

                    String filename = "/" + parts[3] + "/" + parts[4] + ".q";

                    SingleChronicleQueue singleChronicleQueue = fileNameToChronicle.computeIfAbsent
                            (filename, s -> {
                                try {
                                    return new SingleChronicleQueue(filename, 1024);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                return null;
                            });

                    ExcerptAppender excerptappender = singleChronicleQueue.createAppender();

                    QueueAppender qa = new QueueAppender();
                    qa.setCid(clientCID[0]);
                    qa.setCsp(sbCSP);
                    qa.setAppender(excerptappender);
                    cidToQueueAppender.put(clientCID[0], qa);

                    out.writeDocument(false, wireOut -> {
                        wireOut.write(reply).typedMarshallable(qa);
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else if(submit.contentEquals(sb)){
                ExcerptAppender appender = cidToQueueAppender.get(clientCID[0]).getAppender();
                Marshallable m = value.typedMarshallable();
                appender.writeDocument(wire -> wire.write().marshallable(m));
                out.writeDocument(false, wire -> wire.write(index).int64(appender.lastWrittenIndex()));
            }
        };

        in.readDocument(metaDataConsumer, dataConsumer);

        in.clear();
        out.flip();

        System.out.println("Sent by server:");
        System.out.println(Wires.fromSizePrefixedBlobs(out.bytes()));

    }

    public enum Fields implements WireKey{
        methodName,
        tid,
        cid,
        read,
        csp,
        createAppender,
        reply,
        submit,
        index
    }
}
