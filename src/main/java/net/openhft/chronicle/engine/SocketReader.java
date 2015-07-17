/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.engine;

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
