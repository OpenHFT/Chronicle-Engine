package net.openhft.chronicle.engine.server.internal;

import net.openhft.chronicle.engine.api.tree.RequestContextInterner;
import net.openhft.chronicle.network.NetworkContext;
import net.openhft.chronicle.network.WireTcpHandler;
import net.openhft.chronicle.network.connection.CoreFields;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import net.openhft.chronicle.wire.Wires;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static net.openhft.chronicle.network.connection.CoreFields.csp;

/**
 * @author Rob Austin.
 */
public abstract class CspTcpHander<T extends NetworkContext> extends WireTcpHandler<T> {

    protected final StringBuilder cspText = new StringBuilder();
    protected final StringBuilder cpsBuff = new StringBuilder();
    protected final RequestContextInterner requestContextInterner = new RequestContextInterner(128);
    @NotNull
    private final Map<Long, String> cidToCsp = new HashMap<>();
    @NotNull
    private final Map<String, Long> cspToCid = new HashMap<>();
    private long cid;
    private final StringBuilder lastCsp = new StringBuilder();


    protected boolean hasCspChanged(@NotNull final StringBuilder cspText) {
        boolean result = !cspText.equals(lastCsp);

        // if it has changed remember what it changed to, for next time this method is called.
        if (result) {
            lastCsp.setLength(0);
            lastCsp.append(cspText);
        }

        return result;
    }

    public long cid() {
        return cid;
    }

    /**
     * peeks the csp or if it has a cid converts the cid into a Csp and returns that
     */
    protected void readCsp(@NotNull final WireIn wireIn) {
        final StringBuilder event = Wires.acquireStringBuilder();

        cspText.setLength(0);
        final ValueIn read = wireIn.readEventName(event);
        if (csp.contentEquals(event)) {
            read.textTo(cspText);


            tryReadEvent(wireIn, (that, wire) -> {
                final StringBuilder e = Wires.acquireStringBuilder();
                final ValueIn valueIn = wireIn.readEventName(e);
                if (!CoreFields.cid.contentEquals(e))
                    return false;

                final long cid1 = valueIn.int64();
                that.cid = cid1;
                setCid(cspText.toString(), cid1);
                return true;
            });


        } else if (CoreFields.cid.contentEquals(event)) {
            final long cid = read.int64();
            final CharSequence s = getCspForCid(cid);
            cspText.append(s);
            this.cid = cid;
        }
    }


    /**
     * if not successful, in other-words when the function returns try, will return the wire back to
     * the read location
     */
    private void tryReadEvent(@NotNull final WireIn wire,
                              @NotNull final BiFunction<CspTcpHander, WireIn, Boolean> f) {
        final long readPosition = wire.bytes().readPosition();
        boolean success = false;
        try {
            success = f.apply(this, wire);
        } finally {
            if (!success) wire.bytes().readPosition(readPosition);
        }
    }


    public void setCid(String csp, long cid) {
        cidToCsp.put(cid, csp);
    }

    public CharSequence getCspForCid(long cid) {
        return cidToCsp.get(cid);
    }


    @Override
    protected abstract void process(@NotNull WireIn in, @NotNull WireOut out);


}
