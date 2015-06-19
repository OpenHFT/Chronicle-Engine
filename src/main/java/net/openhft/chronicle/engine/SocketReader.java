package net.openhft.chronicle.engine;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Created by Rob on 17/06/15.
 */
public class SocketReader {

   /* ConcurrentLinkedQueue<Response> responses = new ConcurrentLinkedQueue<>();

    class Response {
        long tid;
        Object o;

        public Response(long tid, Object o) {
            this.tid = tid;
            this.o = o;
        }
    }

    ConcurrentHashMap<Long, BlockingQueue> threads = new ConcurrentHashMap<Long, BlockingQueue>();

    private final ThreadLocal<BlockingQueue> queues =
            ThreadLocal.withInitial((Supplier<BlockingQueue>) () -> new ArrayBlockingQueue<>(1));

    public void read(long tid) {



        Queue queue = queues.get();
        threads.put(tid, queue);
        queue.wait(10,TimeUnit.SECONDS);

        try {
            Thread.currentThread().wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void onUpdate() {
        long tid = 0;
        Object o = null;
        responses.add(new Response(tid, o));

        Thread thread = threads.get(tid);
        thread.notify();
    }*/

}
