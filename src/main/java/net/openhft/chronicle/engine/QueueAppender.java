package net.openhft.chronicle.engine;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;

/**
 * Created by daniel on 07/04/15.
 */
public class QueueAppender implements Marshallable {
    private int cid;
    private StringBuilder csp = new StringBuilder();
    private ExcerptAppender appender = null;

    @Override
    public void readMarshallable(WireIn wire) throws IllegalStateException {
        wire.read(QueueWireHandler.Fields.csp).text(csp);
        cid = wire.read(QueueWireHandler.Fields.cid).int32();
    }

    @Override
    public void writeMarshallable(WireOut wire) {
        wire.write(QueueWireHandler.Fields.csp).text(csp);
        wire.write(QueueWireHandler.Fields.cid).int32(cid);
    }

    @Override
    public String toString() {
        return "QueueAppender{" +
                "cid=" + cid +
                ", csp=" + csp +
                '}';
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public StringBuilder getCsp() {
        return csp;
    }

    public void setCsp(StringBuilder csp) {
        this.csp = csp;
    }

    public ExcerptAppender getAppender() {
        return appender;
    }

    public void setAppender(ExcerptAppender appender) {
        this.appender = appender;
    }
}
